// Package config loads command configuration with Viper.
//
// Values use Viper's precedence order: explicit values, flags, environment
// variables, then defaults. Environment variable names use the SQUAD_ prefix
// with dots and hyphens converted to underscores.
package config

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const dashboardAddressDefault = "127.0.0.1:8080"

// Dashboard contains configuration for the standalone dashboard command.
type Dashboard struct {
	Address   string
	TraceFile string
}

type loader struct {
	viper *viper.Viper
	flags *pflag.FlagSet
}

// LoadDashboard parses standalone dashboard flags and environment variables.
func LoadDashboard(args []string) (Dashboard, error) {
	loader := newLoader("squad-dashboard")
	loader.viper.SetDefault("dashboard.addr", dashboardAddressDefault)
	loader.viper.SetDefault("dashboard.trace-file", "")
	loader.flags.String("addr", dashboardAddressDefault, "dashboard listen address")
	loader.flags.String("trace-file", "", "optional JSONL trace file to load and tail into the dashboard")

	if err := loader.bind("dashboard.addr", "addr"); err != nil {
		return Dashboard{}, err
	}
	if err := loader.bind("dashboard.trace-file", "trace-file"); err != nil {
		return Dashboard{}, err
	}
	if err := loader.parse(args); err != nil {
		return Dashboard{}, err
	}

	address := strings.TrimSpace(loader.viper.GetString("dashboard.addr"))
	if address == "" {
		return Dashboard{}, fmt.Errorf("dashboard address is required")
	}

	return Dashboard{
		Address:   address,
		TraceFile: strings.TrimSpace(loader.viper.GetString("dashboard.trace-file")),
	}, nil
}

func newLoader(name string) loader {
	settings := viper.New()
	settings.SetEnvPrefix("SQUAD")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()

	flags := pflag.NewFlagSet(name, pflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return loader{viper: settings, flags: flags}
}

func (l loader) bind(key, flagName string) error {
	if err := l.viper.BindPFlag(key, l.flags.Lookup(flagName)); err != nil {
		return fmt.Errorf("bind %s flag: %w", flagName, err)
	}
	return nil
}

func (l loader) parse(args []string) error {
	if err := l.flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	return nil
}
