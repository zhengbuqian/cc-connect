package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// BoqBinding maps a channel to a boq container instance.
type BoqBinding struct {
	BoqName           string    `json:"boq_name"`
	ChannelName       string    `json:"channel_name,omitempty"`
	OriginalTopicName string    `json:"original_topic_name,omitempty"` // saved on bind, restored on exit
	Runtime           string    `json:"runtime"`                      // "docker" or "podman"
	BoundAt           time.Time `json:"bound_at"`
}

// BoqBindingManager persists channel->boq mappings.
// Top-level key is "project:<name>", second-level key is a channel key.
type BoqBindingManager struct {
	mu                sync.RWMutex
	bindings          map[string]map[string]*BoqBinding
	storePath         string
	lastLoadedModTime time.Time
	lastLoadedSize    int64
}

func NewBoqBindingManager(storePath string) *BoqBindingManager {
	m := &BoqBindingManager{
		bindings:  make(map[string]map[string]*BoqBinding),
		storePath: storePath,
	}
	if storePath != "" {
		m.load()
	}
	return m
}

func (m *BoqBindingManager) Bind(projectKey, channelKey, channelName string, binding *BoqBinding) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
	if m.bindings[projectKey] == nil {
		m.bindings[projectKey] = make(map[string]*BoqBinding)
	}
	binding.ChannelName = channelName
	m.bindings[projectKey][channelKey] = binding
	m.saveLocked()
}

func (m *BoqBindingManager) Unbind(projectKey, channelKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
	if proj := m.bindings[projectKey]; proj != nil {
		for _, candidate := range workspaceChannelKeyCandidates(channelKey) {
			delete(proj, candidate)
		}
		if len(proj) == 0 {
			delete(m.bindings, projectKey)
		}
	}
	m.saveLocked()
}

func (m *BoqBindingManager) Lookup(projectKey, channelKey string) *BoqBinding {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
	return m.lookupLocked(projectKey, channelKey)
}

func (m *BoqBindingManager) lookupLocked(projectKey, channelKey string) *BoqBinding {
	proj := m.bindings[projectKey]
	if proj == nil {
		return nil
	}
	for _, candidate := range workspaceChannelKeyCandidates(channelKey) {
		if b := proj[candidate]; b != nil {
			return b
		}
	}
	return nil
}

func (m *BoqBindingManager) saveLocked() {
	if m.storePath == "" {
		return
	}
	data, err := json.MarshalIndent(m.bindings, "", "  ")
	if err != nil {
		slog.Error("boq bindings: marshal error", "err", err)
		return
	}
	if err := AtomicWriteFile(m.storePath, data, 0o644); err != nil {
		slog.Error("boq bindings: save error", "err", err)
		return
	}
	if info, err := os.Stat(m.storePath); err == nil {
		m.lastLoadedModTime = info.ModTime()
		m.lastLoadedSize = info.Size()
	}
}

func (m *BoqBindingManager) load() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
}

func (m *BoqBindingManager) refreshLocked() {
	if m.storePath == "" {
		return
	}
	info, err := os.Stat(m.storePath)
	if err != nil {
		if os.IsNotExist(err) {
			m.bindings = make(map[string]map[string]*BoqBinding)
			m.lastLoadedModTime = time.Time{}
			m.lastLoadedSize = 0
			return
		}
		slog.Error("boq bindings: stat error", "err", err)
		return
	}
	if !m.lastLoadedModTime.IsZero() && info.ModTime().Equal(m.lastLoadedModTime) && info.Size() == m.lastLoadedSize {
		return
	}

	data, err := os.ReadFile(m.storePath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("boq bindings: load error", "err", err)
		}
		return
	}
	loaded := make(map[string]map[string]*BoqBinding)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &loaded); err != nil {
			slog.Error("boq bindings: unmarshal error", "err", err)
			return
		}
	}
	m.bindings = loaded
	m.lastLoadedModTime = info.ModTime()
	m.lastLoadedSize = info.Size()
}

// --- Boq container discovery and exec helpers ---

// listBoqContainerNames returns the names of all running boq containers
// (containers whose name starts with "boq-"), with the "boq-" prefix stripped.
// It checks docker first, then podman. This always runs on the host.
func listBoqContainerNames() []string {
	var names []string
	seen := make(map[string]bool)
	for _, runtime := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(runtime); err != nil {
			continue
		}
		out, err := exec.Command(runtime, "ps", "--format", "{{.Names}}", "--filter", "name=^boq-").CombinedOutput()
		if err != nil {
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			name := strings.TrimPrefix(line, "boq-")
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	return names
}

// --- Container runtime detection and exec helpers ---

// DetectContainerRuntime checks whether a boq container is running and returns
// the runtime ("docker" or "podman") that manages it.
func DetectContainerRuntime(containerName string) (string, error) {
	for _, runtime := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(runtime); err != nil {
			continue
		}
		out, err := exec.Command(runtime, "inspect", "--format", "{{.State.Running}}", containerName).CombinedOutput()
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(out)) == "true" {
			return runtime, nil
		}
	}
	return "", fmt.Errorf("container %q is not running (checked docker and podman)", containerName)
}

// ContainerExecPrefix builds the command prefix for executing inside a container.
// The returned slice should be prepended to the actual command.
func ContainerExecPrefix(runtime, containerName, workDir string, interactive bool) []string {
	args := []string{runtime, "exec"}
	if interactive {
		args = append(args, "-i")
	}
	if workDir != "" {
		args = append(args, "-w", workDir)
	}
	args = append(args, containerName)
	return args
}

// ContainerExecCommand creates an exec.Cmd that runs a shell command inside a container.
func ContainerExecCommand(ctx context.Context, runtime, containerName, workDir, shellCmd string) *exec.Cmd {
	prefix := ContainerExecPrefix(runtime, containerName, workDir, false)
	prefix = append(prefix, "sh", "-c", shellCmd)
	return exec.CommandContext(ctx, prefix[0], prefix[1:]...)
}

// ContainerDirExists checks whether a directory exists inside the container.
func ContainerDirExists(ctx context.Context, runtime, containerName, dir string) bool {
	prefix := ContainerExecPrefix(runtime, containerName, "", false)
	prefix = append(prefix, "test", "-d", dir)
	cmd := exec.CommandContext(ctx, prefix[0], prefix[1:]...)
	return cmd.Run() == nil
}

// TopicRenamer is an optional Platform interface for renaming topic/chat titles.
type TopicRenamer interface {
	RenameTopic(ctx context.Context, replyCtx any, name string) error
	TopicName(ctx context.Context, replyCtx any) string
}

// ContainerExecSetter is an optional Agent interface for injecting a container
// exec prefix that wraps the agent CLI binary invocation.
type ContainerExecSetter interface {
	SetContainerExec(prefix []string)
}
