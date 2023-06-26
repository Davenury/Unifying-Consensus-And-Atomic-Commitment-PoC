package commands

import (
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
)

type DeployConfig struct {
	NumberOfPeersInPeersets []int
	NumberOfPeersV2         int
	PeersetsConfigurationV2 string
	DeployNamespace         string
	DeployCreateNamespace   bool
	WaitForReadiness        bool
	ImageName               string
	IsMetricTest            bool
	CreateResources         bool
	ProxyDelay              DelayConfig
	ProxyLimit              string
	MonitoringNamespace     string
}

func (cfg *DeployConfig) version() string {
	if cfg.NumberOfPeersV2 != 0 {
		return "v2"
	}
	return "v1"
}

func (cfg *DeployConfig) GetPeersAndPeersets() (string, string) {
	if cfg.version() == "v2" {
		peers, _ := utils.GenerateServicesForPeersStaticPort([]int{cfg.NumberOfPeersV2}, servicePort)
		peersets := cfg.PeersetsConfigurationV2
		return peers, peersets
	}
	return utils.GenerateServicesForPeersStaticPort(cfg.NumberOfPeersInPeersets, servicePort)
}

func (cfg *DeployConfig) GetDeployFunction() func(config DeployConfig, peers string, peersets string, experimentUUID uuid.UUID) {
	if cfg.version() == "v2" {
		return DoDeployV2
	}
	return DoDeployV1
}

type Config struct {
	monitoringNamespace            string
	createMonitoringNamespace      bool
	numberOfPeersInPeersets        []int
	numberOfPeersV2                int
	peersetsConfigurationV2        string
	createTestNamespace            bool
	testNamespace                  string
	applicationImageName           string
	performanceImage               string
	singleRequestsNumber           int
	multipleRequestsNumber         int
	testDuration                   string
	maxPeersetsInChange            int
	testsSendingStrategy           string
	testsCreatingChangeStrategy    string
	pushgatewayAddress             string
	enforceAcUsage                 bool
	acProtocol                     string
	consensusProtocol              string
	performanceTestTimeoutDeadline string
	cleanupAfterWork               bool
	isMetricTest                   bool
	deployMonitoring               bool
	constantLoad                   string
	fixedPeersetsInChange          string
	proxyLimit                     string
	loadGeneratorType              string
	increasingLoadBound            float64
	increasingLoadIncreaseDelay    string
	increasingLoadIncreaseStep     float64
	enforceConsensusLeader         bool
	ProxyDelay                     DelayConfig
}

type DelayConfig struct {
	delayType                         string
	fixedProxyDelay                   string
	randomProxyDelayStart             string
	randomProxyDelayEnd               string
	ownPeersetProxyDelay              string
	otherPeersetProxyDelay            string
	ownPeersetRandomProxyDelayStart   string
	ownPeersetRandomProxyDelayEnd     string
	otherPeersetRandomProxyDelayStart string
	otherPeersetRandomProxyDelayEnd   string
}

func (cfg *DelayConfig) getDelayConfigmap() []apiv1.EnvVar {
	switch cfg.delayType {
	case "fixed":
		return []apiv1.EnvVar{
			{
				Name:  "TYPE",
				Value: "FIXED_DELAY",
			},
			{
				Name:  "DELAY",
				Value: cfg.fixedProxyDelay,
			},
		}
	case "random":
		return []apiv1.EnvVar{
			{
				Name:  "TYPE",
				Value: "RANDOM_DELAY",
			},
			{
				Name:  "START_DELAY",
				Value: cfg.randomProxyDelayStart,
			},
			{
				Name:  "END_DELAY",
				Value: cfg.randomProxyDelayEnd,
			},
		}
	case "peerset-fixed":
		return []apiv1.EnvVar{
			{
				Name:  "TYPE",
				Value: "PEERSET_BASED_FIXED_DELAY",
			},
			{
				Name:  "MY_PEERSET_DELAY",
				Value: cfg.ownPeersetProxyDelay,
			},
			{
				Name:  "NOT_MY_PEERSET_DELAY",
				Value: cfg.otherPeersetProxyDelay,
			},
		}
	case "peerset-random":
		return []apiv1.EnvVar{
			{
				Name:  "TYPE",
				Value: "PEERSET_BASED_RANDOM_DELAY",
			},
			{
				Name:  "MY_PEERSET_START_DELAY",
				Value: cfg.ownPeersetRandomProxyDelayStart,
			},
			{
				Name:  "MY_PEERSET_END_DELAY",
				Value: cfg.ownPeersetRandomProxyDelayEnd,
			},
			{
				Name:  "NOT_MY_PEERSET_START_DELAY",
				Value: cfg.otherPeersetRandomProxyDelayStart,
			},
			{
				Name:  "NOT_MY_PEERSET_END_DELAY",
				Value: cfg.otherPeersetRandomProxyDelayEnd,
			},
		}
	default:
		panic(fmt.Sprintf("Delay %s is not defined", cfg.delayType))
	}
}
