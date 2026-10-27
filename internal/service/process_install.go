package service

import "strings"

func (m *manager) writeProcessUnits(initial string) error {
	for _, name := range []string{Name + ".service", Name + "-init.service"} {
		raw, err := units.ReadFile("units/" + name)
		if err != nil {
			return err
		}
		raw = []byte(strings.ReplaceAll(string(raw), "@BINARY@", initial))
		raw = []byte(strings.ReplaceAll(string(raw), "@MAINTENANCE@", initial))
		if err := atomicWrite(m.path("/etc/systemd/system/"+name), raw, 0644); err != nil {
			return err
		}
	}
	return nil
}
