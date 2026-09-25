package astro

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestCronIntervalMins(t *testing.T) {
	cases := []struct {
		raw  string
		want float64
		ok   bool
	}{
		{"", 0, false},
		{"@daily", 1440, true},
		{"@hourly", 60, true},
		{"0 5 * * *", 1440, true},
		{"*/15 * * * *", 15, true},
		{"0 * * * *", 60, true},
		{"0 */4 * * *", 240, true},
		{"0 10,15 * * *", 720, true},
		{`{"__type":"CronExpression","value":"0 5 * * *"}`, 1440, true},
		{"0 5 * * 1", 10080, true},
	}
	for _, tc := range cases {
		got := CronIntervalMins(tc.raw)
		if !tc.ok {
			if got != nil {
				t.Fatalf("%q: want nil got %v", tc.raw, *got)
			}
			continue
		}
		if got == nil || *got != tc.want {
			t.Fatalf("%q: got %v want %v", tc.raw, got, tc.want)
		}
	}
}

func TestKeepDAGDropsStaleAndUntaggedPaused(t *testing.T) {
	active := true
	stale := dagAPI{IsStale: true, IsActive: &active}
	if keepDAG(stale, domainFlags(false, false, true)) {
		t.Fatal("stale")
	}
	inactive := false
	if keepDAG(dagAPI{IsActive: &inactive}, domainFlags(false, false, true)) {
		t.Fatal("inactive")
	}
	paused := dagAPI{IsPaused: true, IsActive: &active}
	if keepDAG(paused, domainFlags(false, false, false)) {
		t.Fatal("untagged paused")
	}
	if !keepDAG(paused, domainFlags(true, false, false)) {
		t.Fatal("custom paused should stay")
	}
	if !keepDAG(dagAPI{IsPaused: false, IsActive: &active}, domainFlags(false, false, false)) {
		t.Fatal("active unpaused")
	}
}

func TestInferCustomFromDagID(t *testing.T) {
	d := domain.DAG{DAGID: "dbt_inorbit_custom"}
	inferCustomFromID(&d)
	if !d.IsCustom {
		t.Fatal("expected custom token")
	}
	plain := domain.DAG{DAGID: "dbt_inorbit_daily"}
	inferCustomFromID(&plain)
	if plain.IsCustom {
		t.Fatal("daily is not custom")
	}
}

func TestDagAvailableRequiresARunUnlessCustom(t *testing.T) {
	if dagAvailable(domain.DAG{DAGID: "ghost"}) {
		t.Fatal("empty DAG")
	}
	if !dagAvailable(domain.DAG{DAGID: "adhoc", IsCustom: true}) {
		t.Fatal("custom")
	}
	if !dagAvailable(domain.DAG{DAGID: "ok", Status: "SUCCESS"}) {
		t.Fatal("status")
	}
}

func domainFlags(custom, primary, sla bool) domain.DAG {
	d := domain.DAG{IsCustom: custom, IsPrimary: primary}
	if sla {
		v := 60.0
		d.IntervalMins = &v
	}
	return d
}
