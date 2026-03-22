const API_KEY = "sai-r2-secret-2026";

export default {
  async fetch(request, env) {

    // Auth check - first thing
    const authHeader = request.headers.get("X-API-Key");
    if (authHeader !== API_KEY) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), {
        status: 401,
        headers: { "Content-Type": "application/json" }
      });
    }

    const MAX_STORAGE_BYTES = 8 * 1024 * 1024 * 1024;
    const pathname = new URL(request.url).pathname;
    const key = pathname.slice(1);

    if (request.method === "PUT" || request.method === "POST") {
      const currentBytes = parseInt(await env.R2_STORAGE_TRACKER.get("total_bytes") || "0");
      const contentLength = parseInt(request.headers.get("content-length") || "0");

      if (currentBytes + contentLength > MAX_STORAGE_BYTES) {
        return new Response(JSON.stringify({
          error: "Storage limit reached! 8GB quota exceeded.",
          usedGB: (currentBytes / 1024 / 1024 / 1024).toFixed(2),
          limitGB: 8
        }), {
          status: 507,
          headers: { "Content-Type": "application/json" }
        });
      }

      await env.MY_BUCKET.put(key, request.body);

      const newTotal = currentBytes + contentLength;
      await env.R2_STORAGE_TRACKER.put("total_bytes", String(newTotal));

      return new Response(JSON.stringify({
        success: true,
        key,
        usedGB: (newTotal / 1024 / 1024 / 1024).toFixed(2),
        remainingGB: ((MAX_STORAGE_BYTES - newTotal) / 1024 / 1024 / 1024).toFixed(2)
      }), { headers: { "Content-Type": "application/json" } });
    }

    if (request.method === "DELETE") {
      const object = await env.MY_BUCKET.head(key);

      if (object) {
        const currentBytes = parseInt(await env.R2_STORAGE_TRACKER.get("total_bytes") || "0");
        await env.MY_BUCKET.delete(key);
        const newTotal = Math.max(0, currentBytes - object.size);
        await env.R2_STORAGE_TRACKER.put("total_bytes", String(newTotal));
        return new Response(JSON.stringify({ success: true, freedGB: (object.size / 1024 / 1024 / 1024).toFixed(4) }),
          { headers: { "Content-Type": "application/json" } });
      }
      return new Response(JSON.stringify({ error: "File not found" }), { status: 404 });
    }

    if (request.method === "GET") {
      // Storage usage stats
      if (pathname === "/usage") {
        const currentBytes = parseInt(await env.R2_STORAGE_TRACKER.get("total_bytes") || "0");
        return new Response(JSON.stringify({
          usedBytes: currentBytes,
          usedGB: (currentBytes / 1024 / 1024 / 1024).toFixed(2),
          limitGB: 8,
          remainingGB: ((MAX_STORAGE_BYTES - currentBytes) / 1024 / 1024 / 1024).toFixed(2),
          percentUsed: ((currentBytes / MAX_STORAGE_BYTES) * 100).toFixed(1) + "%"
        }), { headers: { "Content-Type": "application/json" } });
      }

      // Download file by key (e.g. GET /unified_snapshots.csv)
      const object = await env.MY_BUCKET.get(key);
      if (!object) {
        return new Response(JSON.stringify({ error: "File not found", key }), {
          status: 404,
          headers: { "Content-Type": "application/json" }
        });
      }
      return new Response(object.body, {
        headers: {
          "Content-Type": "text/csv",
          "Content-Disposition": `attachment; filename="${key}"`,
        }
      });
    }

    return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405 });
  }
};