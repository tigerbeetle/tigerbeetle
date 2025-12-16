// Adds reference to sub-folders containing the external (non-Go) files
// required to build the TigerBeetle client. Otherwise the `tb_client.h`
// header and library object files would be pruned during `go mod vendor`.
package native
