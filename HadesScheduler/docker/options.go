package docker

import (
	"fmt"

	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/moby/moby/client"
)

// DockerOption configures a Scheduler during construction (functional options
// pattern). Pass any number to NewScheduler; each is applied in order and may
// return an error to abort construction.
type DockerOption func(*Scheduler) error

// WithDockerHost points the scheduler at a specific Docker daemon endpoint.
func WithDockerHost(dockerHost string) DockerOption {
	return func(s *Scheduler) error {
		cli, err := client.New(client.WithHost(dockerHost))
		if err != nil {
			return fmt.Errorf("creating Docker client with host %s: %w", dockerHost, err)
		}
		s.cli = cli
		return nil
	}
}

// WithLogPublisher sets the publisher used to emit container logs (rejects nil).
func WithLogPublisher(publisher buildlogs.LogPublisher) DockerOption {
	return func(s *Scheduler) error {
		if publisher == nil {
			return fmt.Errorf("nil publisher provided")
		}
		s.logPublisher = publisher
		return nil
	}
}

// WithStatusPublisher sets the publisher used to emit job status transitions (rejects nil).
func WithStatusPublisher(publisher buildstatus.StatusPublisher) DockerOption {
	return func(s *Scheduler) error {
		if publisher == nil {
			return fmt.Errorf("nil publisher provided")
		}
		s.statusPublisher = publisher
		return nil
	}
}

// WithScriptExecutor sets the shell used to run each step's script (e.g. "/bin/bash -c").
func WithScriptExecutor(scriptExecutor string) DockerOption {
	return func(s *Scheduler) error {
		s.scriptExecutor = scriptExecutor
		return nil
	}
}

// WithContainerAutoremove controls whether step containers are removed on exit.
// Keep it false to retain container logs after a run.
func WithContainerAutoremove(autoremove bool) DockerOption {
	return func(s *Scheduler) error {
		s.containerAutoremove = autoremove
		return nil
	}
}

// WithCPULimit sets the default CPU limit (whole CPUs) for step containers that
// do not specify their own.
func WithCPULimit(cpuLimit uint) DockerOption {
	return func(s *Scheduler) error {
		s.cpuLimit = cpuLimit
		return nil
	}
}

// WithMemoryLimit sets the default memory limit (e.g. "4g") for step containers
// that do not specify their own.
func WithMemoryLimit(memoryLimit string) DockerOption {
	return func(s *Scheduler) error {
		s.memoryLimit = memoryLimit
		return nil
	}
}

// WithImagePullPolicy sets whether steps contact the registry for images that
// are already present locally. See PullPolicy. An unrecognised value fails the
// scheduler's construction rather than falling back to a default, so a
// misconfigured policy is reported at startup instead of quietly doing nothing.
func WithImagePullPolicy(policy string) DockerOption {
	return func(s *Scheduler) error {
		p, err := ParsePullPolicy(policy)
		if err != nil {
			return err
		}
		s.imagePullPolicy = p
		return nil
	}
}
