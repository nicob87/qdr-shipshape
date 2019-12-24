package send_receive

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/rh-messaging/qdr-shipshape/pkg/spec/interconnect"
	"github.com/rh-messaging/shipshape/pkg/api/client/amqp"
	"github.com/rh-messaging/shipshape/pkg/api/client/amqp/qeclients"
	//"github.com/rh-messaging/shipshape/pkg/framework"
	//"github.com/rh-messaging/shipshape/pkg/framework/log"
	//"strconv"
	"github.com/rh-messaging/shipshape/pkg/framework/log"
)

const (
	MessageCount int = 100
)

var _ = Describe("Exchanges AnyCast messages across the nodes", func() {

	It("Exchanges small messages", func() {
		ctx := _Framework.GetFirstContext()
		By("Deploying one Python sender and one Python receiver")

		url := fmt.Sprintf("amqp://%s:5672/anycastAddress", interconnect.GetDefaultServiceName(IcInteriorRouterName, ctx.Namespace))

		psBuilder := qeclients.NewSenderBuilder("sender-"+IcInteriorRouterName, qeclients.Python, *ctx, url)
		psBuilder.Messages(MessageCount)
		psBuilder.MessageContentFromFile(ConfigMapName, "small-message.txt")
		sdr, err := psBuilder.Build()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(sdr).NotTo(gomega.BeNil())

		rBuilder := qeclients.NewReceiverBuilder("receiver-"+IcInteriorRouterName, qeclients.Python, *ctx, url)
		rBuilder.Messages(MessageCount)
		rcv, err := rBuilder.Build()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(rcv).NotTo(gomega.BeNil())

		err = sdr.Deploy()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = rcv.Deploy()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		sdr.Wait()
		gomega.Expect(sdr.Status()).To(gomega.Equal(amqp.Success))

		rcv.Wait()
		gomega.Expect(rcv.Status()).To(gomega.Equal(amqp.Success))

		log.Logf("Sender %s - Results - Delivered: %d - Released: %d - Modified: %d",
			sdr.Name, sdr.Result().Delivered, sdr.Result().Released, sdr.Result().Modified)

		log.Logf("Receiver %s - Results - Delivered: %d - Released: %d - Modified: %d",
			rcv.Name, rcv.Result().Delivered, rcv.Result().Released, rcv.Result().Modified)

		gomega.Expect(rcv.Result().Delivered).To(gomega.Equal(MessageCount))
		gomega.Expect(sdr.Result().Delivered).To(gomega.Equal(MessageCount))

	})
})
