package docker

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
}
