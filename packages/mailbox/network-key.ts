/** Qualify ledger identities so records from different networks cannot satisfy each other's obligations. */
export function netKey(networkId: string, id: string): string {
  return `${networkId}\0${id}`;
}
