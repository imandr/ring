Message Types
=============

Direct Message
--------------

Direct message is sent to a specific node.

**fast** - sent over edge and diagonal links. Fast. Can cross between link types. Guaranteed to reach the destination.

**sequential** - sent over edge links only. Slow. Guaranteed to reach the destination.

Broadcast Message
-----------------

Broadcast messgae is sent to all network nodes.

**fast unconfirmed** - sent over both diagonal and edge links. Can cross both ways. Fastest. 
Guaranteed to reach all nodes. Can not be confirmed.

**fast confirmed** - sent over both diagonal and edge links. Can not cross from diagonal to edge. Fast. 
Guaranteed to reach all nodes. Can be confirmed.

**sequential** - sent over edge links only. Slow. Guaranteed to reach all nodes sequentially. Can be confirmed.

==============      ================= ========= ======== ============ ============== =============
 Class              Type               Flags     Speed    Guaranteed   Can confirm    Sequential
==============      ================= ========= ======== ============ ============== =============
 Direct             fast               EDed      fast     yes          no             no
 Direct             sequential         E-e-      slow     yes          no             yes
 Broadcast          fast unconfirmed   EDed      fastest  yes          no             no
 Broadcast          fast confirmed     ED-d      fast    yes          yes            no
 Broadcast          slow confirmed     E-e-      slow     yes          yes            yes
==============      ================= ========= ======== ============ ============== =============
 
 
  









