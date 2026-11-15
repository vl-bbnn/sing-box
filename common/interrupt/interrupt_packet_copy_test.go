package interrupt

import (
 "bytes"
 "encoding/binary"
 "errors"
 "io"
 "net"
 "os"
 "testing"

 vmess "github.com/sagernet/sing-vmess"
 "github.com/sagernet/sing/common/buf"
 "github.com/sagernet/sing/common/bufio"
 M "github.com/sagernet/sing/common/metadata"
)

// The real server copy path has no handshake-failure reporter on a mux reader.
// It can return nil after a first-packet write failure. Assert bytes as well.
type originalPacketReader struct { destination M.Socksaddr; count int }
func (r *originalPacketReader) ReadPacket(b *buf.Buffer) (M.Socksaddr,error) {
 if r.count==2{return M.Socksaddr{},io.EOF}
 r.count++;p:=make([]byte,20);binary.BigEndian.PutUint16(p,1);binary.BigEndian.PutUint32(p[4:],0x2112a442);p[19]=byte(r.count)
 _,err:=b.Write(p);return r.destination,err
}
func TestGroupedXUDPServerCopyDoesNotHidePacketLoss(t *testing.T) {
 for _,address:=range []string{"192.0.2.1:3478","[2001:db8::1]:3478","stun.example.com:3478"} {
  for _,depth:=range []int{0,1,3} {
   target:=M.ParseSocksaddr(address);wire:=new(packetWire);var conn net.PacketConn=vmess.NewXUDPConn(wire,target)
   groups:=make([]*Group,depth);for i:=range groups{groups[i]=NewGroup();conn=groups[i].NewPacketConn(conn,false)}
   n,err:=bufio.CopyPacket(bufio.NewPacketConn(conn),&originalPacketReader{destination:target})
   if target.IsDomain() && depth>0 && os.Getenv("EXPECT_DOMAIN_FAILURE")=="1" {
    if err!=nil || n!=0 || wire.Len()!=0{t.Fatalf("expected suppressed first-write error with zero original bytes: n=%d err=%v wire=%d",n,err,wire.Len())}
   } else {
    if !errors.Is(err,io.EOF) || n!=40{t.Fatalf("original packets incomplete: n=%d err=%v",n,err)}
    encoded:=bytes.NewReader(wire.Bytes())
    for i:=0;i<2;i++ {
     var length uint16;if e:=binary.Read(encoded,binary.BigEndian,&length);e!=nil{t.Fatal(e)}
     header:=make([]byte,5);if _,e:=io.ReadFull(encoded,header);e!=nil{t.Fatal(e)}
     if header[2]!=byte(i+1) || header[3]!=1 || header[4]!=vmess.NetworkUDP{t.Fatal("invalid frame")}
     destination,e:=vmess.AddressSerializer.ReadAddrPort(encoded);if e!=nil || destination!=target{t.Fatalf("destination changed: %v %v",destination,e)}
     if e=binary.Read(encoded,binary.BigEndian,&length);e!=nil{t.Fatal(e)}
     payload:=make([]byte,length);if _,e=io.ReadFull(encoded,payload);e!=nil{t.Fatal(e)}
     if len(payload)!=20 || binary.BigEndian.Uint32(payload[4:])!=0x2112a442 || payload[19]!=byte(i+1){t.Fatal("original datagram changed")}
    }
    if encoded.Len()!=0{t.Fatal("unexpected trailing bytes")}
   }
   if e:=conn.Close();e!=nil{t.Fatal(e)};for _,g:=range groups{g.Interrupt(true)}
   if wire.closed!=1{t.Fatal("close ownership changed")}
  }
 }
}
