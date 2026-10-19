// Package appconfig loads process settings from a YAML file. It never reads
// environment variables or expands placeholders.
package appconfig

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"aiagent-gateway/internal/gateway"
	"go.yaml.in/yaml/v3"
)

const DefaultPath = "config.yaml"
const SystemPath = "/etc/aiagent-gateway/config.yaml"

type Config struct {
	AdminToken         string `yaml:"admin_token"`
	DataDir            string `yaml:"data_dir"`
	Port               string `yaml:"port"`
	AdminPort          string `yaml:"admin_port"`
	PublicURL          string `yaml:"public_url"`
	MaxInflight        int    `yaml:"max_inflight"`
	MaxPerCaller       int    `yaml:"max_per_caller"`
	MaxPerEndpoint     int    `yaml:"max_per_endpoint"`
	MaxSessions        int    `yaml:"max_sessions"`
	MaxTasks           int    `yaml:"max_tasks"`
	MaxResources       int    `yaml:"max_resources"`
	MaxResourceAliases int    `yaml:"max_resource_aliases"`
	MaxDataIdempotency int    `yaml:"max_data_idempotency"`
	StateSocket        string `yaml:"state_socket"`
	EntrySocket        string `yaml:"entry_socket"`
	ControlSocket      string `yaml:"control_socket"`
	Deployment         string `yaml:"deployment"`
}

func Defaults() Config {
	return Config{
		DataDir: "./data", Port: "8080", AdminPort: "9090",
		MaxInflight: 4096, MaxPerCaller: 1024, MaxPerEndpoint: 256,
		MaxSessions: 100000, MaxTasks: 100000, MaxResources: 200000,
		MaxResourceAliases: 200000, MaxDataIdempotency: 100000,
		StateSocket: "/run/aiagent-gateway/state.sock", EntrySocket: "/run/aiagent-gateway/entry.sock",
		ControlSocket: "/run/aiagent-gateway-control/controller.sock",
		Deployment:    "/etc/aiagent-gateway/deployment.json",
	}
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open YAML config: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return Config{}, fmt.Errorf("read YAML config: %w", err)
	}
	if len(raw) > 1<<20 {
		return Config{}, fmt.Errorf("YAML config exceeds 1 MiB")
	}
	c, err := Parse(raw)
	if err != nil {
		return Config{}, err
	}
	// Relative paths are based on the config file, independent of process cwd.
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Config{}, err
	}
	for _, p := range []*string{&c.DataDir, &c.StateSocket, &c.EntrySocket, &c.ControlSocket, &c.Deployment} {
		if !filepath.IsAbs(*p) {
			*p = filepath.Join(base, *p)
		}
	}
	return c, nil
}

func Parse(raw []byte) (Config, error) {
	c := Defaults()
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return Config{}, fmt.Errorf("invalid YAML config: expected a mapping")
	}
	if containsNull(&doc) {
		return Config{}, fmt.Errorf("invalid YAML config: null values are not supported; omit optional fields instead")
	}
	d := yaml.NewDecoder(bytes.NewReader(raw))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		// YAML type errors can contain the offending scalar, including secrets.
		return Config{}, fmt.Errorf("invalid YAML config: check field names, duplicate keys and value types")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, fmt.Errorf("YAML config must contain exactly one document")
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func containsNull(n *yaml.Node) bool {
	if n.Tag == "!!null" {
		return true
	}
	for _, child := range n.Content {
		if containsNull(child) {
			return true
		}
	}
	return false
}

func (c Config) Validate() error {
	if err := gateway.ValidateOptions(c.Options()); err != nil {
		return err
	}
	for name, port := range map[string]string{"port": c.Port, "admin_port": c.AdminPort} {
		n, err := strconv.Atoi(port)
		if err != nil || strings.Trim(port, "0123456789") != "" || n < 1 || n > 65535 {
			return fmt.Errorf("%s must be a port number from 1 to 65535", name)
		}
	}
	for name, n := range map[string]int{
		"max_inflight": c.MaxInflight, "max_per_caller": c.MaxPerCaller, "max_per_endpoint": c.MaxPerEndpoint,
		"max_sessions": c.MaxSessions, "max_tasks": c.MaxTasks, "max_resources": c.MaxResources,
		"max_resource_aliases": c.MaxResourceAliases, "max_data_idempotency": c.MaxDataIdempotency,
	} {
		if n < 1 {
			return fmt.Errorf("%s must be a positive integer", name)
		}
	}
	for name, path := range map[string]string{"data_dir": c.DataDir, "state_socket": c.StateSocket, "entry_socket": c.EntrySocket, "control_socket": c.ControlSocket, "deployment": c.Deployment} {
		if strings.TrimSpace(path) == "" || strings.ContainsRune(path, 0) {
			return fmt.Errorf("%s must be a nonempty path", name)
		}
	}
	return nil
}

func (c Config) ListenAddress() string {
	return net.JoinHostPort("", c.Port)
}

func (c Config) AdminListenAddress() string {
	return net.JoinHostPort("", c.AdminPort)
}

func (c Config) Options() gateway.Options {
	return gateway.Options{
		DataDir: c.DataDir, AdminToken: c.AdminToken, PublicURL: c.PublicURL,
		MaxInflight: c.MaxInflight, MaxPerCaller: c.MaxPerCaller, MaxPerEndpoint: c.MaxPerEndpoint,
		MaxSessions: c.MaxSessions, MaxTasks: c.MaxTasks, MaxResources: c.MaxResources,
		MaxResourceAliases: c.MaxResourceAliases, MaxDataIdempotency: c.MaxDataIdempotency,
	}
}
