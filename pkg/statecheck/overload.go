package overloadcheck

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	util_log "github.com/grafana/loki/pkg/util/log"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
)

var ServiceIngesterOverload bool = false

var p_memlimit int = 100
var p_memlimit_revocer int = 70
var docker_mode string = "true"

func Start() error {
	err := initLimit()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				ServiceIngesterOverload = check()
				/*
				if ServiceIngesterOverload {
					err := downloadFile("./../log/heap."+strconv.FormatInt(time.Now().Unix(), 10), "http://172.21.72.17:5180/debug/pprof/heap")
					level.Info(util_log.Logger).Log("getheap error", err)
				}
				*/
				for ServiceIngesterOverload {
					for i := 0; i < 30; i++ {
						select {
						case <-time.After(1 * time.Second):
							ServiceIngesterOverload = !checkRecover()
						}
						if ServiceIngesterOverload {
							break
						}
					}
				}
			}
		}
	}()

	return nil
}

func initLimit() error {
	docker_mode = os.Getenv("DOCKER_MODE")

	var err_conv error
	p_memlimit_str := os.Getenv("LOKI_MEMORY_LIMIT")
	p_memlimit, err_conv = strconv.Atoi(p_memlimit_str)
	if err_conv != nil || p_memlimit < 40 {
		level.Error(util_log.Logger).Log("msg", "invalid LOKI_MEMORY_LIMIT", p_memlimit_str, err_conv)
		return errors.New("invalid LOKI_MEMORY_LIMIT")
	}
	if p_memlimit < 50 {
		p_memlimit_revocer = p_memlimit
	} else {
		p_memlimit_revocer = p_memlimit - 10
	}
	level.Info(util_log.Logger).Log("p_memlimit", p_memlimit, "p_memlimit_revocer", p_memlimit_revocer)
	return nil
}

func check() bool {
	percent := (uint)(0)
	if docker_mode != "true" {
		meminfo, _ := mem.VirtualMemory()
		percent = (uint)(meminfo.UsedPercent)
		level.Info(util_log.Logger).Log("memory.LimitPercent", p_memlimit, "memory.UsedPercent", percent)
	} else {
		p, _ := process.NewProcess(int32(os.Getpid()))
		memlimit, _ := readUint("/sys/fs/cgroup/memory/memory.limit_in_bytes")
		meminfo, _ := p.MemoryInfo()
		percent = (uint)(meminfo.RSS * 100 / memlimit)
		level.Info(util_log.Logger).Log("memory.limit_in_bytes", memlimit, "meminfo.RSS", meminfo.RSS, "memory.LimitPercent", p_memlimit, "memory.UsedPercent", percent)
	}
	if percent >= (uint)(p_memlimit) {
		errinfo := fmt.Sprintf("memory limit to:%d, cur_memory_use_percent:%v", p_memlimit, percent)
		level.Warn(util_log.Logger).Log("msg", "memory limit", "err", errinfo)
		return true
	}
	return false
}

func checkRecover() bool {
	percent := (uint)(0)
	if docker_mode != "true" {
		meminfo, _ := mem.VirtualMemory()
		percent = (uint)(meminfo.UsedPercent)
		level.Info(util_log.Logger).Log("memory.LimitRecoverPercent", p_memlimit_revocer, "memory.UsedPercent", percent)
	} else {
		p, _ := process.NewProcess(int32(os.Getpid()))
		memlimit, _ := readUint("/sys/fs/cgroup/memory/memory.limit_in_bytes")
		meminfo, _ := p.MemoryInfo()
		percent = (uint)(meminfo.RSS * 100 / memlimit)
		level.Info(util_log.Logger).Log("memory.limit_in_bytes", memlimit, "meminfo.RSS", meminfo.RSS, "memory.LimitRecoverPercent", p_memlimit_revocer, "memory.UsedPercent", percent)
	}
	if percent >= (uint)(p_memlimit_revocer) {
		errinfo := fmt.Sprintf("memory limit reeover to:%d, cur_memory_use_percent:%v", p_memlimit_revocer, percent)
		level.Info(util_log.Logger).Log("msg", "memory limit", "err", errinfo)
		return false
	}
	level.Info(util_log.Logger).Log("recover at", percent)
	return true
}

func readUint(path string) (uint64, error) {
	v, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return parseUint(strings.TrimSpace(string(v)), 10, 64)
}

func parseUint(s string, base, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		intValue, intErr := strconv.ParseInt(s, base, bitSize)
		// 1. Handle negative values greater than MinInt64 (and)
		// 2. Handle negative values lesser than MinInt64
		if intErr == nil && intValue < 0 {
			return 0, nil
		} else if intErr != nil &&
			intErr.(*strconv.NumError).Err == strconv.ErrRange &&
			intValue < 0 {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func downloadFile(filepath string, url string) (err error) {
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
