package send_receive

import (
	"github.com/interconnectedcloud/qdr-operator/pkg/apis/interconnectedcloud/v1alpha1"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/rh-messaging/qdr-shipshape/pkg/messaging"
	//"github.com/rh-messaging/qdr-shipshape/pkg/spec/interconnect"
	"github.com/rh-messaging/shipshape/pkg/apps/qdrouterd/deployment"
	"github.com/rh-messaging/shipshape/pkg/apps/qdrouterd/qdrmanagement"
	"github.com/rh-messaging/shipshape/pkg/apps/qdrouterd/qdrmanagement/entities"
	"github.com/rh-messaging/shipshape/pkg/framework"
	"github.com/rh-messaging/shipshape/pkg/framework/operators"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	_Framework *framework.Framework

	IcInteriorRouter *v1alpha1.InterconnectSpec

	ConfigMap *v1.ConfigMap
)

const (
	IcInteriorRouterName = "interior"
	ConfigMapName        = "messaging-files"
)

// Creates a unique namespace prefixed as "e2e-tests-smoke"
var _ = ginkgo.BeforeEach(func() {
	// Initializes using only Qdr Operator
	_Framework = framework.NewFrameworkBuilder("smoke").WithBuilders(operators.SupportedOperators[operators.OperatorTypeQdr]).Build()
})

// Initializes the Interconnect (CR) specs to be deployed
var _ = ginkgo.JustBeforeEach(func() {

	ctx := _Framework.GetFirstContext()

	// Generates a config map with messaging files (content) to be
	// used by the AMQP QE Clients
	generateMessagingFilesConfigMap()

	IcInteriorRouter = defaultInteriorSpec()

	deployInterconnect(ctx, IcInteriorRouterName, IcInteriorRouter) //esto es un spec

	validateNetwork(IcInteriorRouterName)
})

func generateMessagingFilesConfigMap() {
	var err error
	ctx := _Framework.GetFirstContext()
	ConfigMap, err = ctx.Clients.KubeClient.CoreV1().ConfigMaps(ctx.Namespace).Create(&v1.ConfigMap{
		ObjectMeta: v12.ObjectMeta{
			Name: ConfigMapName,
		},
		Data: map[string]string{
			"small-message.txt":  messaging.GenerateMessageContent("ThisIsARepeatableMessage", 1024),
			"medium-message.txt": messaging.GenerateMessageContent("ThisIsARepeatableMessage", 1024*100),
			"large-message.txt":  messaging.GenerateMessageContent("ThisIsARepeatableMessage", 1024*500),
		},
	})
	gomega.Expect(err).To(gomega.BeNil())
	gomega.Expect(ConfigMap).NotTo(gomega.BeNil())
}

func validateNetwork(interiorName string) {

	ginkgo.By("Validating network on " + interiorName)
	ctx := _Framework.GetFirstContext()

	podList, err := ctx.ListPodsForDeploymentName(interiorName)
	gomega.Expect(err).To(gomega.BeNil())

	for _, pod := range podList.Items {
		// Expect that the 4 interior routers are showing up
		nodes, err := qdrmanagement.QdmanageQueryWithRetries(*ctx, pod.Name, entities.Node{}, 10,
			60, nil, func(es []entities.Entity, err error) bool {
				return err != nil || len(es) == 1
			})
		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(nodes)).To(gomega.Equal(1))
	}
}

func deployInterconnect(ctx *framework.ContextData, icName string, icSpec *v1alpha1.InterconnectSpec) {
	// Deploying Interconnect using provided context
	ic, err := deployment.CreateInterconnectFromSpec(*ctx, icSpec.DeploymentPlan.Size, icName, *icSpec)
	gomega.Expect(err).To(gomega.BeNil())
	gomega.Expect(ic).NotTo(gomega.BeNil())

	// Wait for Interconnect deployment
	err = framework.WaitForDeployment(ctx.Clients.KubeClient, ctx.Namespace, icName, int(icSpec.DeploymentPlan.Size), framework.RetryInterval, framework.Timeout)
	gomega.Expect(err).To(gomega.BeNil())
}

func defaultInteriorSpec() *v1alpha1.InterconnectSpec {
	// TODO Define a standard configuration file that allows images to be customized
	return &v1alpha1.InterconnectSpec{
		DeploymentPlan: v1alpha1.DeploymentPlanType{
			Size:      1,
			Image:     "quay.io/interconnectedcloud/qdrouterd:latest",
			Role:      "interior",
			Placement: "Any",
		},
	}
}
