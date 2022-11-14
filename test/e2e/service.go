package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/utils/pointer"

	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
)

var _ = ginkgo.Describe("Services", func() {
	const (
		serviceName     = "testservice"
		pythonWebServer = `#!/bin/bash
cat <<'EOF' > /tmp/server.py
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import parse
from random import choice
from string import ascii_lowercase
import logging

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()

        payload_size="100"

        path = self.path
        o = parse.urlparse(path)
        qs = parse.parse_qs(o.query)
        if 'payload_size' in qs:
            payload_size = qs['payload_size'][0]

        string_val = "".join(choice(ascii_lowercase) for i in range(int(payload_size)))
        if 'search_string' in qs:
            search_string = qs['search_string'][0]
            string_val = "".join(search_string for i in range(int(int(payload_size)/len(search_string))))

        logging.warning("Sending response %%s\n" %% string_val)
        self.wfile.write(bytes(string_val, "utf8"))

print("Starting server")
with HTTPServer(('', %d), handler) as server:
    server.serve_forever()
EOF
python3 /tmp/server.py`
		pythonWebServerImage = "registry.fedoraproject.org/fedora:latest"
		podPortMin           = 9800
		podPortMax           = 9899
		servicePortMin       = 31200
		servicePortMax       = 31299
	)

	f := wrappedTestFramework("services")

	var cs clientset.Interface

	ginkgo.BeforeEach(func() {
		cs = f.ClientSet
	})
	cleanupFn := func() {}

	ginkgo.AfterEach(func() {
		cleanupFn()
	})

	udpPort := int32(rand.Intn(1000) + 10000)
	udpPortS := fmt.Sprintf("%d", udpPort)

	ginkgo.It("Creates a host-network service, and ensures that host-network pods can connect to it", func() {
		namespace := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)

		ginkgo.By("Creating a ClusterIP service")
		service, err := jig.CreateUDPService(func(s *v1.Service) {
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "udp",
					Protocol:   v1.ProtocolUDP,
					Port:       80,
					TargetPort: intstr.FromInt(int(udpPort)),
				},
			}
		})
		framework.ExpectNoError(err)

		ginkgo.By("creating a host-network backend pod")

		serverPod := e2epod.NewAgnhostPod(namespace, "backend", nil, nil, []v1.ContainerPort{{ContainerPort: (udpPort)}, {ContainerPort: (udpPort), Protocol: "UDP"}},
			"netexec", "--udp-port="+udpPortS)
		serverPod.Labels = jig.Labels
		serverPod.Spec.HostNetwork = true

		serverPod = f.PodClient().CreateSync(serverPod)
		nodeName := serverPod.Spec.NodeName

		ginkgo.By("Connecting to the service from another host-network pod on node " + nodeName)
		// find the ovn-kube node pod on this node
		pods, err := cs.CoreV1().Pods("ovn-kubernetes").List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app=ovnkube-node",
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		framework.ExpectNoError(err)
		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		clientPod := pods.Items[0]

		cmd := fmt.Sprintf(`/bin/sh -c 'echo hostname | /usr/bin/socat -t 5 - "udp:%s"'`,
			net.JoinHostPort(service.Spec.ClusterIP, "80"))

		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			stdout, err := framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return stdout == nodeName, nil
		})
		framework.ExpectNoError(err)
	})

	// Set up a hostNetwork pod on ovn-control-plane.
	// Set up a nodePort service.
	// E.g.: Query from ovn-worker2 to the service on ovn-worker that targets the pod on ovn-control-plane.
	ginkgo.When("A nodePort service load-balancing to a hostNetworked pod is created", func() {
		var podNode string
		var serviceNodeInternalIPs []string
		var clientPod v1.Pod
		var deployment appsv1.Deployment
		var podPort int
		var podName string
		var svc v1.Service
		var servicePort int

		ginkgo.BeforeEach(func() {
			framework.Logf("Selecting 3 schedulable nodes")
			nodes, err := e2enode.GetBoundedReadySchedulableNodes(f.ClientSet, 3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Find 1st node to run the hostNetwork web server on.
			podNode = nodes.Items[0].Name
			// Find 2nd node to run the service query against.
			serviceNodeInternalIPs = e2enode.GetAddresses(&nodes.Items[1], v1.NodeInternalIP)
			gomega.Expect(len(serviceNodeInternalIPs)).To(gomega.BeNumerically(">", 0))
			// Find ovnkube-node pod on 3rd node from which we will run the curl to the service.
			pods, err := cs.CoreV1().Pods("ovn-kubernetes").List(context.TODO(), metav1.ListOptions{
				LabelSelector: "app=ovnkube-node",
				FieldSelector: "spec.nodeName=" + nodes.Items[2].Name,
			})
			framework.ExpectNoError(err)
			gomega.Expect(pods.Items).To(gomega.HaveLen(1))
			clientPod = pods.Items[0]

			gomega.Eventually(func() error {
				podPort = rand.Intn(podPortMax-podPortMin) + podPortMin
				podName = fmt.Sprintf("test-pod-%d", podPort)
				framework.Logf("Creating the hostNetwork pod listening on TCP port %d", podPort)
				deployment = appsv1.Deployment{
					TypeMeta:   metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{Name: podName},
					Spec: appsv1.DeploymentSpec{
						Replicas: pointer.Int32Ptr(1),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"app": podName,
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									"app": podName,
								},
							},
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "fedora",
										Image: pythonWebServerImage,
										Command: []string{
											"/bin/bash",
											"-xc",
											fmt.Sprintf(pythonWebServer, podPort),
										},
									},
								},
								HostNetwork: true,
								NodeName:    podNode,
							},
						},
					},
				}
				_, err := f.ClientSet.AppsV1().Deployments(f.Namespace.Name).Create(context.TODO(), &deployment, metav1.CreateOptions{})
				return err
			}).Should(gomega.Succeed())

			gomega.Eventually(func() error {
				servicePort = rand.Intn(servicePortMax-servicePortMin) + servicePortMin
				framework.Logf("Creating the nodePort service listening on TCP port %d and targeting pod port %d",
					servicePort, podPort)
				svc = v1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "test-service"},
					Spec: v1.ServiceSpec{
						Ports: []v1.ServicePort{
							{
								NodePort: int32(servicePort),
								Port:     int32(podPort),
								Protocol: v1.ProtocolTCP,
							},
						},
						Selector: map[string]string{"app": podName},
						Type:     v1.ServiceTypeNodePort},
				}
				_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(context.TODO(), &svc, metav1.CreateOptions{})
				return err
			}).Should(gomega.Succeed())

		})
		ginkgo.It("Queries to the nodePort service shall work", func() {
			for _, serviceNodeIP := range serviceNodeInternalIPs {
				cmd := fmt.Sprintf("curl --max-time 10 -g -q -s 'http://%s:%d?payload_size=10'", serviceNodeIP, servicePort)
				_, err := framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 60*time.Second)
				framework.ExpectNoError(err)
			}
		})
		ginkgo.It("Queries to the nodePort service with large replies shall work", func() {
			for _, serviceNodeIP := range serviceNodeInternalIPs {
				cmd := fmt.Sprintf("curl --max-time 10 -g -q -s 'http://%s:%d?payload_size=10000'", serviceNodeIP, servicePort)
				_, err := framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 60*time.Second)
				framework.ExpectNoError(err)

			}
		})
	})

	// This test checks a special case: we add another IP address on the node *and* manually set that
	// IP address in to endpoints. It is used for some special apiserver hacks by remote cluster people.
	// So, ensure that it works for pod -> service and host -> service traffic
	ginkgo.It("All service features work when manually listening on a non-default address", func() {
		namespace := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(cs, e2eservice.MaxNodesForEndpointsTests)
		framework.ExpectNoError(err)
		node := nodes.Items[0]
		nodeName := node.Name
		pods, err := cs.CoreV1().Pods("ovn-kubernetes").List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app=ovnkube-node",
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		framework.ExpectNoError(err)
		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		clientPod := &pods.Items[0]

		ginkgo.By("Using node" + nodeName + " and pod " + clientPod.Name)

		ginkgo.By("Creating an empty ClusterIP service")
		service, err := jig.CreateUDPService(func(s *v1.Service) {
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "udp",
					Protocol:   v1.ProtocolUDP,
					Port:       80,
					TargetPort: intstr.FromInt(int(udpPort)),
				},
			}

			s.Spec.Selector = nil // because we will manage the endpoints ourselves
		})
		framework.ExpectNoError(err)

		ginkgo.By("Adding an extra IP address to the node's loopback interface")
		isV6 := strings.Contains(service.Spec.ClusterIP, ":")
		octet := rand.Intn(255)
		extraIP := fmt.Sprintf("192.0.2.%d", octet)
		extraCIDR := extraIP + "/32"
		if isV6 {
			extraIP = fmt.Sprintf("fc00::%d", octet)
			extraCIDR = extraIP + "/128"
		}

		cmd := fmt.Sprintf(`ip -br addr; ip addr del %s dev lo; ip addr add %s dev lo; ip -br addr`, extraCIDR, extraCIDR)
		_, err = framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
		framework.ExpectNoError(err)
		cleanupFn = func() {
			cmd := fmt.Sprintf(`ip addr del %s dev lo || true`, extraCIDR)
			_, err = framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
		}

		ginkgo.By("Starting a UDP server listening on the additional IP")
		// now that 2.2.2.2 exists on the node's lo interface, let's start a server listening on it
		// we use UDP here since agnhost lets us pick the listen address only for UDP
		serverPod := e2epod.NewAgnhostPod(namespace, "backend", nil, nil, []v1.ContainerPort{{ContainerPort: (udpPort)}, {ContainerPort: (udpPort), Protocol: "UDP"}},
			"netexec", "--udp-port="+udpPortS, "--udp-listen-addresses="+extraIP)
		serverPod.Labels = jig.Labels
		serverPod.Spec.NodeName = nodeName
		serverPod.Spec.HostNetwork = true
		serverPod.Spec.Containers[0].TerminationMessagePolicy = v1.TerminationMessageFallbackToLogsOnError
		f.PodClient().CreateSync(serverPod)

		ginkgo.By("Ensuring the server is listening on the additional IP")
		// Connect from host -> additional IP. This shouldn't touch OVN at all, just acting as a basic
		// sanity check that we're actually listening on this IP
		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			cmd = fmt.Sprintf(`echo hostname | /usr/bin/socat -t 5 - "udp:%s"`,
				net.JoinHostPort(extraIP, udpPortS))
			stdout, err := framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return (stdout == nodeName), nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Adding this IP as a manual endpoint")
		_, err = f.ClientSet.CoreV1().Endpoints(namespace).Create(context.TODO(),
			&v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{Name: service.Name},
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP: extraIP,
							},
						},
						Ports: []v1.EndpointPort{
							{
								Name:     "udp",
								Port:     udpPort,
								Protocol: "UDP",
							},
						},
					},
				},
			},
			metav1.CreateOptions{},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Confirming that the service is accesible via the service IP from a host-network pod")
		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			cmd = fmt.Sprintf(`/bin/sh -c 'echo hostname | /usr/bin/socat -t 5 - "udp:%s"'`,
				net.JoinHostPort(service.Spec.ClusterIP, "80"))
			stdout, err := framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return stdout == nodeName, nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Confirming that the service is accessible from the node's pod network")
		// Now, spin up a pod-network pod on the same node, and ensure we can talk to the "local address" service
		clientServerPod := e2epod.NewAgnhostPod(namespace, "client", nil, nil, []v1.ContainerPort{{ContainerPort: (udpPort)}, {ContainerPort: (udpPort), Protocol: "UDP"}},
			"netexec")
		clientServerPod.Spec.NodeName = nodeName
		f.PodClient().CreateSync(clientServerPod)
		clientServerPod, err = f.PodClient().Get(context.TODO(), clientServerPod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)

		// annoying: need to issue a curl to the test pod to tell it to connect to the service
		err = wait.PollImmediate(framework.Poll, 30*time.Second, func() (bool, error) {
			cmd = fmt.Sprintf("curl -g -q -s 'http://%s/dial?request=%s&protocol=%s&host=%s&port=%d&tries=1'",
				net.JoinHostPort(clientServerPod.Status.PodIP, "8080"),
				"hostname",
				"udp",
				service.Spec.ClusterIP,
				80)
			stdout, err := framework.RunHostCmdWithRetries(clientPod.Namespace, clientPod.Name, cmd, framework.Poll, 30*time.Second)
			if err != nil {
				return false, err
			}
			return stdout == fmt.Sprintf(`{"responses":["%s"]}`, nodeName), nil
		})
		framework.ExpectNoError(err)
	})
})
