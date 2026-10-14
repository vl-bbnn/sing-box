package interrupt

import (
 "bytes"
 "encoding/binary"
 "io"
 "net"
 "os"
 "strings"
 "testing"
 "time"

 vmess "github.com/sagernet/sing-vmess"
 "github.com/sagernet/sing/common/buf"
 "github.com/sagernet/sing/common/bufio"
 M "github.com/sagernet/sing/common/metadata"
)

// Inject into a source-bound copy of common/interrupt. No live credentials,
// routes or servers; the real XUDP writer emits original STUN-sized datagrams.
type packetWire struct { bytes.Buffer; closed int }
func (w *packetWire) Read(p []byte) (int,error) { return 0,io.EOF }
func (w *packetWire) Close() error { w.closed++; return nil }
func (*packetWire) LocalAddr() net.Addr { return &net.TCPAddr{} }
func (*packetWire) RemoteAddr() net.Addr { return &net.TCPAddr{} }
func (*packetWire) SetDeadline(time.Time) error { return nil }
func (*packetWire) SetReadDeadline(time.Time) error { return nil }
func (*packetWire) SetWriteDeadline(time.Time) error { return nil }

func TestGroupedXUDPOriginalDomainDatagrams(t *testing.T) {
 for _, address := range []string{"192.0.2.1:3478","[2001:db8::1]:3478","stun.example.com:3478"} {
  for _, depth := range []int{0,1,3} {
   t.Run(address+strings.Repeat("-group",depth),func(t *testing.T) {
    target:=M.ParseSocksaddr(address); wire:=new(packetWire)
    var conn net.PacketConn=vmess.NewXUDPConn(wire,target)
    groups:=make([]*Group,depth)
    for i:=range groups { groups[i]=NewGroup();conn=groups[i].NewPacketConn(conn,false) }
    writer:=bufio.NewPacketConn(conn)
    payload:=make([]byte,20);binary.BigEndian.PutUint16(payload,1);binary.BigEndian.PutUint32(payload[4:],0x2112a442)
    for i:=0;i<2;i++ {
     payload[19]=byte(i+1);packet:=buf.NewSize(1024);packet.Resize(512,0);packet.Write(payload)
     err:=writer.WritePacket(packet,target)
     if target.IsDomain() && depth>0 && os.Getenv("EXPECT_DOMAIN_FAILURE")=="1" {
      if err==nil || !strings.Contains(err.Error(),"unsupported address") { t.Fatalf("baseline failure missing: %v",err) };return
     }
     if err!=nil { t.Fatal(err) }
     encoded:=bytes.NewReader(wire.Bytes());var frameLength uint16
     if err=binary.Read(encoded,binary.BigEndian,&frameLength);err!=nil {t.Fatal(err)}
     header:=make([]byte,5);if _,err=io.ReadFull(encoded,header);err!=nil {t.Fatal(err)}
     if header[2]!=byte(i+1) || header[3]!=1 || header[4]!=vmess.NetworkUDP {t.Fatalf("wrong XUDP frame: %v",header)}
     destination,err:=vmess.AddressSerializer.ReadAddrPort(encoded);if err!=nil {t.Fatal(err)}
     if destination!=target || int(frameLength)!=5+vmess.AddressSerializer.AddrPortLen(target) {t.Fatalf("destination changed: %v",destination)}
     var length uint16;if err=binary.Read(encoded,binary.BigEndian,&length);err!=nil {t.Fatal(err)}
     actual:=make([]byte,length);if _,err=io.ReadFull(encoded,actual);err!=nil {t.Fatal(err)}
     if !bytes.Equal(actual,payload) || encoded.Len()!=0 {t.Fatal("original datagram changed")};wire.Reset()
    }
    if err:=conn.Close();err!=nil {t.Fatal(err)}
    for _,g:=range groups {g.Interrupt(true)}
    if wire.closed!=1 {t.Fatalf("already closed connection interrupted again: %d",wire.closed)}
   })
  }
 }
}
