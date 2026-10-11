// A ring of SIZE processes: each process forwards a token to the next one.
// pid 0 builds the ring tail-first, injects the token, and reports the trip.

const SIZE = 1000;

let next = self();
for (let i = SIZE - 1; i > 0; i--) {
  const target = next;
  next = spawn(`
    const n = await recv();
    send(${target}, n + 1);
  `);
}

const t0 = Date.now();
send(next, 0);
// A worker that exits without forwarding would hang the root, so bound the wait.
const hops = await recv(10000).catch((e) => {
  if (e instanceof TimeoutError) return null;
  throw e;
});
if (hops === null) console.log("token lost: not back within 10 s");
else console.log(`token passed through ${hops} workers in ${Date.now() - t0} ms (ring of ${SIZE} processes incl. pid 0)`);
