package docker

import "time"

type EnvConfig struct {
	DockerHost           string `env:"DOCKER_HOST" envDefault:"unix:///var/run/docker.sock"`
	ContainerAutoremove  bool   `env:"DOCKER_CONTAINER_AUTOREMOVE" envDefault:"false"`
	DockerScriptExecutor string `env:"DOCKER_SCRIPT_EXECUTOR" envDefault:"/bin/bash -c"`
	CPULimit             uint   `env:"DOCKER_CPU_LIMIT"`    // Number of CPUs - e.g. '6'
	MemoryLimit          string `env:"DOCKER_MEMORY_LIMIT"` // RAM usage in g or m  - e.g. '4g'

	// ImagePullPolicy controls whether a step contacts the registry when its
	// image is already in the local daemon. See PullPolicy for the values and
	// why the default is "always".
	ImagePullPolicy string `env:"DOCKER_IMAGE_PULL_POLICY" envDefault:"always"`

	// ImagePullFreshness lets a job skip the registry for a locally present
	// image that this scheduler pulled successfully within the window, e.g.
	// "30s". Zero keeps "always" meaning every job. Concurrent pulls of the
	// same image are shared regardless of this setting.
	ImagePullFreshness time.Duration `env:"DOCKER_IMAGE_PULL_FRESHNESS" envDefault:"0s"`
}
