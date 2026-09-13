//go:build with_wlt

package libbox

import (
 "testing"
 "time"
 "github.com/sagernet/sing/common/control"
)

func TestRootExternalCloseMustJoinBlockedCallback(t *testing.T) {
 iface := &control.Interface{Index:9,Name:"rmnet_data0"}
 manager := &coordinatorTestManager{finder:coordinatorTestFinder{iface:iface}}
 monitor := newCoordinatorTestMonitor(manager,iface)
 c := newPhysicalLinkCoordinator(monitor)
 c.update(iface.Name,int32(iface.Index),false,false)
 entered,release,exited:=make(chan struct{}),make(chan struct{}),make(chan struct{})
 monitor.RegisterCallback(func(*control.Interface,int){close(entered);<-release;close(exited)})
 c.forward(lossObservation(c,false))
 select{case <-entered:case <-time.After(time.Second):t.Fatal("callback not entered")}
 closed:=make(chan struct{})
 go func(){c.close();close(closed)}()
 early:=false
 select{case <-closed:early=true;case <-time.After(30*time.Millisecond):}
 close(release)
 <-exited
 select{case <-closed:case <-time.After(time.Second):t.Fatal("external close did not finish")}
 if early{t.Fatal("external close returned while callback was still blocked")}
}
