package k8s

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/hades-scheduler/hades/hadesScheduler/log"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const JobNameLabel = "job-name=%s"

type PodLogReader struct {
	K8sClient kubernetes.Interface
	Namespace string
	JobID     string
	Publisher log.NATSPublisher
}

// Processes logs for a specific container in the pod
// Fetches logs, parses them, and publishes to NATS
// Used in both Scheduler and Operator modes
func (pl PodLogReader) ProcessContainerLogs(ctx context.Context, podName string, containerName string) error {
	slog.Info("Getting container logs", "pod", podName, "container", containerName)
	stdout, stderr, err := pl.getContainerLogs(ctx, podName, containerName)
	if err != nil {
		return fmt.Errorf("getting container logs: %w", err)
	}

	slog.Info("Parsing container logs", "pod", podName, "container", containerName)
	parser := log.NewStdLogParser(stdout, stderr)
	buildJobLog, err := parser.ParseContainerLogs(containerName, pl.JobID)
	if err != nil {
		return fmt.Errorf("parsing container logs: %w", err)
	}

	buildJobLog.JobID = pl.JobID
	slog.Info("Publishing logs", "pod", podName, "container", containerName)
	return pl.Publisher.PublishJobLog(ctx, buildJobLog)
}

// Helper function for ProcessContainerLogs
// Fetches logs for a specific container in the pod using container name
// Used in both Scheduler and Operator modes
func (pl PodLogReader) getContainerLogs(ctx context.Context, podName string, containerName string) (*bytes.Buffer, *bytes.Buffer, error) {
	// get logs of <container name>
	podLogOpts := corev1.PodLogOptions{
		Container:  containerName,
		Follow:     false,
		Timestamps: true,
	}

	req := pl.K8sClient.CoreV1().Pods(pl.Namespace).GetLogs(podName, &podLogOpts)
	logReader, err := req.Stream(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
			slog.Debug("Log stream canceled (expected on job completion)", "pod", podName, "container", containerName)
			return new(bytes.Buffer), new(bytes.Buffer), context.Canceled
		}
		return nil, nil, fmt.Errorf("getting container logs: %w", err)
	}
	defer logReader.Close()

	// K8s logs are already combined (stdout/stderr mixed)
	// So we need to parse them differently
	allLogs := new(bytes.Buffer)
	if _, err := io.Copy(allLogs, logReader); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
			slog.Debug("Log copy canceled (expected on job completion)", "pod", podName, "container", containerName)
			return allLogs, new(bytes.Buffer), nil
		}
		return nil, nil, fmt.Errorf("reading logs: %w", err)
	}

	// For K8s, you might need to separate stdout/stderr differently
	// Currently all logs are treated as stdout for simplicity
	return allLogs, new(bytes.Buffer), nil
}

// StreamContainerLogs follows a container's logs and publishes them
// incrementally as they are produced, rather than reading the whole log once
// after the container terminates. It blocks until the follow stream closes (the
// container stopped) or ctx is cancelled.
//
// sinceTime, when non-nil, is passed as PodLogOptions.SinceTime so a re-established
// stream (e.g. after an operator restart) skips lines already published. progress,
// when non-nil, is called with the timestamp of the last published line so the
// caller can persist a restart offset.
//
// Kubernetes merges a container's stdout and stderr into a single stream, so all
// lines are tagged as stdout, matching the buffered reader's behavior.
func (pl PodLogReader) StreamContainerLogs(ctx context.Context, podName, containerName string, sinceTime *metav1.Time, progress func(time.Time)) error {
	podLogOpts := corev1.PodLogOptions{
		Container:  containerName,
		Follow:     true,
		Timestamps: true,
		SinceTime:  sinceTime,
	}

	req := pl.K8sClient.CoreV1().Pods(pl.Namespace).GetLogs(podName, &podLogOpts)
	stream, err := req.Stream(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
			slog.Debug("Log stream canceled (expected on job completion)", "pod", podName, "container", containerName)
			return context.Canceled
		}
		return fmt.Errorf("streaming container logs: %w", err)
	}
	defer stream.Close()

	return log.StreamContainerLogs(ctx, &pl.Publisher, pl.JobID, containerName,
		log.StreamOptions{Progress: progress},
		log.StreamSource{Reader: stream, StreamType: log.StreamStdout},
	)
}

// ErrPodNotFound is returned by ResolvePod while the Job has no pod, e.g.
// because it is suspended or the Job controller has not created the pod yet.
var ErrPodNotFound = errors.New("pod not found")

// ResolvePod returns the pod the Job controller created for the given JobID.
// Pods are named <job>-<random>, so they can only be found through the
// job-name label the Job controller puts on them.
// Used in both Scheduler and Operator modes
func (pl PodLogReader) ResolvePod(ctx context.Context) (*corev1.Pod, error) {
	jobName := fmt.Sprintf(buildlogs.JobNamePrefix, pl.JobID)
	lst, err := pl.K8sClient.CoreV1().Pods(pl.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf(JobNameLabel, jobName),
	})
	if err != nil {
		return nil, fmt.Errorf("listing pods for job %s: %w", jobName, err)
	}
	switch len(lst.Items) {
	case 0:
		return nil, fmt.Errorf("pod for jobID %s: %w", pl.JobID, ErrPodNotFound)
	case 1:
		return &lst.Items[0], nil
	default:
		return nil, fmt.Errorf("found multiple pods with label job-name=%s; expected exactly 1", jobName)
	}
}
