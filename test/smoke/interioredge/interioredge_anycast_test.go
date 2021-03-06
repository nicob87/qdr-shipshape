package interioredge

import (
	"bufio"
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/rh-messaging/qdr-shipshape/pkg/clients/python"
	"github.com/rh-messaging/shipshape/pkg/api/client/amqp"
	"github.com/rh-messaging/shipshape/pkg/apps/qdrouterd/qdrmanagement"
	"github.com/rh-messaging/shipshape/pkg/apps/qdrouterd/qdrmanagement/entities"
	"github.com/rh-messaging/shipshape/pkg/framework"
	"github.com/rh-messaging/shipshape/pkg/framework/log"
	"io"
	v1 "k8s.io/api/core/v1"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	SnapshotRouterLinks = false
	SnapshotDelay = 10 * time.Second
	WG sync.WaitGroup
)

var _ = Describe("Exchange AnyCast messages across all nodes", func() {

	const (
		totalSmall    = 100000
		totalMedium   = 10000
		totalLarge    = 1000
	)

	var (
		//allRouterNames = []string{"interior-east"}
		allRouterNames = TopologySmoke.AllRouterNames()
	)

	It(fmt.Sprintf("exchanges %d small messages with 1kb using senders and receivers across all router nodes", totalSmall), func() {
		runAnycastTest(totalSmall, 1024, allRouterNames)
	})

	//
	// TODO continue with the investigation after F2F
	// Disabling medium and large message tests (after F2F I [Fernando] will continue with the investigation).
	//

	//It(fmt.Sprintf("exchanges %d medium messages with 100kb using %d senders and receivers", totalMedium, totalSenders), func() {
	//	runAnycastTest(totalMedium, 1024*100, allRouterNames)
	//})

	//It(fmt.Sprintf("exchanges %d large messages with 500kb using %d senders and receivers", totalLarge, totalSenders), func() {
	//	runAnycastTest(totalLarge, 1024*500, allRouterNames)
	//})

})

func runAnycastTest(msgCount int, msgSize int, allRouterNames []string) {

	const (
		anycastAddress = "anycast/smoke/interioredge"
		//timeout        = 180
		timeout        = 600
	)
	ctx := TopologySmoke.FrameworkSmoke.GetFirstContext()
	SnapshotRouterLinks = IsDebugEnabled()

	// Reading number of clients from config or use default of 1
	numClients, _ := Config.GetEnvPropertyInt("NUMBER_CLIENTS", 1)

	// Deploying all senders across all nodes
	By("Deploying senders across all router nodes")
	senders := []*python.PythonClient{}
	for _, routerName := range allRouterNames {
		sndName := fmt.Sprintf("sender-pythonbasic-%s", routerName)
		senders = append(senders, python.DeployPythonClient(ctx, routerName, sndName, anycastAddress, IsDebugEnabled(), python.BasicSender, numClients, msgCount, msgSize, timeout)...)
	}

	// Deploying all receivers across all nodes
	By("Deploying receivers across all router nodes")
	receivers := []*python.PythonClient{}
	for _, routerName := range allRouterNames {
		rcvName := fmt.Sprintf("receiver-pythonbasic-%s", routerName)
		receivers = append(receivers, python.DeployPythonClient(ctx, routerName, rcvName, anycastAddress, IsDebugEnabled(), python.BasicReceiver, numClients, msgCount, msgSize, timeout)...)
	}

	//TODO maybe remove this or make it a reusable component
	if SnapshotRouterLinks {
		snapshotRouters(allRouterNames, ctx, anycastAddress, WG)
	}

	//TODO split this function into more cohesive ones
	type results struct {
		delivered int
		success   bool
	}
	sndResults := []results{}
	rcvResults := []results{}

	By("Collecting senders results")
	for _, s := range senders {
		log.Logf("Waiting sender: %s", s.Name)
		s.Wait()

		log.Logf("Parsing sender results")
		res := s.Result()
		log.Logf("Sender %s - Status: %v - Results - Delivered: %d - Released: %d - Rejected: %d - Modified: %d - Accepted: %d",
			s.Name, s.Status(), res.Delivered, res.Released, res.Rejected, res.Modified, res.Accepted)

		// Adding sender results
		totalSent := res.Delivered - res.Rejected - res.Released
		sndResults = append(sndResults, results{delivered: totalSent, success: s.Status() == amqp.Success})

		if IsDebugEnabled() {
			saveLogs(s.Pod.Name)
		}
	}

	By("Collecting receivers results")
	for _, r := range receivers {
		log.Logf("Waiting receiver: %s", r.Name)
		r.Wait()

		log.Logf("Parsing receiver results")
		res := r.Result()
		log.Logf("Receiver %s - Status: %v - Results - Delivered: %d", r.Name, r.Status(), res.Delivered)

		// Adding receiver results
		rcvResults = append(rcvResults, results{delivered: res.Delivered, success: r.Status() == amqp.Success})

		if IsDebugEnabled() {
			saveLogs(r.Pod.Name)
		}
	}

	if IsDebugEnabled() {
		for _, r := range allRouterNames {
			pods, err := ctx.ListPodsForDeploymentName(r)
			if err != nil {
				log.Logf("Error retrieving pods: %v", err)
				continue
			}
			for _, p := range pods.Items {
				saveLogs(p.Name)
			}
		}
	}

	// At this point, we should stop snapshoting the routers
	SnapshotRouterLinks = false
	WG.Wait()

	// Validating total number of messages sent/received
	By("Validating sender results")
	for _, s := range sndResults {
		gomega.Expect(s.success).To(gomega.BeTrue())
		gomega.Expect(s.delivered).To(gomega.Equal(msgCount))
	}
	By("Validating receiver results")
	for _, r := range rcvResults {
		gomega.Expect(r.success).To(gomega.BeTrue())
		gomega.Expect(r.delivered).To(gomega.Equal(msgCount))
	}

}

func snapshotRouters(allRouterNames []string, ctx *framework.ContextData, anycastAddress string, wg sync.WaitGroup) {
	for _, routerName := range allRouterNames {
		pods, err := ctx.ListPodsForDeploymentName(routerName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pod := range pods.Items {
			wg.Add(1)
			go func(podName string) {
				defer wg.Done()
				logFile := fmt.Sprintf("/tmp/ns_%s_pod_%s_linkStats.log", ctx.Namespace, podName)
				log.Logf("Saving linkStatus to: %s", logFile)
				f, _ := os.Create(logFile)
				defer f.Close()
				w := bufio.NewWriter(f)

				// Once all senders/receivers stopped, this should become false
				for SnapshotRouterLinks {
					// Retrieve links from router for the related address
					links, err := qdrmanagement.QdmanageQuery(*ctx, podName, entities.Link{}, func(entity entities.Entity) bool {
						l := entity.(entities.Link)
						return strings.HasSuffix(l.OwningAddr, anycastAddress)
					})
					if err != nil {
					log.Logf("Error querying router: %v", err)
					}

					if len(links) > 0 {
						for _, e := range links {
							stat, err := json.Marshal(e)
							if err != nil {
								log.Logf("Error marshalling link: %v", err)
								continue
							}
							w.Write(stat)
							w.WriteString("\n")
							w.Flush()
						}
					}

					time.Sleep(SnapshotDelay)
				}

				log.Logf("Finished snapshoting router - Logfile: %s", logFile)
			}(pod.Name)
		}
	}
}

// TODO Move this to some sort of debug package (contextdata must be an argument)
//      as well, as not all tests are supposed to run in a single namespace/cluster.
func saveLogs(podName string) {

	// Wait so pod has enough time to finish properly
	time.Sleep(5 * time.Second)

	ctx := TopologySmoke.FrameworkSmoke.GetFirstContext()
	request := ctx.Clients.KubeClient.CoreV1().Pods(ctx.Namespace).GetLogs(podName, &v1.PodLogOptions{})
	logs, err := request.Stream()
	if err != nil {
		log.Logf("ERROR getting stream - %v", err)
		return
	}

	// Close when done reading
	defer logs.Close()

	// Iterate through lines\
	logFile := fmt.Sprintf("/tmp/ns_%s_pod_%s.log", ctx.Namespace, podName)
	log.Logf("Saving pod logs to: %s", logFile)
	f, _ := os.Create(logFile)
	defer f.Close()
	w := bufio.NewWriter(f)

	// Saving logs
	_, err = io.Copy(w, logs)

}
