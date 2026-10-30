// モデルの全メジャーを 1 つずつ評価し、エラーになるメジャーを列挙する（環境変数 PBI_TOKEN）
const fs = require("fs");
const [WS, DS, MAP] = process.argv.slice(2);
const names = Object.keys(JSON.parse(fs.readFileSync(MAP, "utf8")));
(async () => {
  const bad = [];
  for (const n of names) {
    const query = `EVALUATE ROW ( "v", [${n.replace(/]/g, "]]")}] )`;
    const res = await fetch(`https://api.powerbi.com/v1.0/myorg/groups/${WS}/datasets/${DS}/executeQueries`, {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.PBI_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify({ queries: [{ query }] }),
    });
    const j = await res.json();
    const err = j.error || (j.results && j.results[0].error);
    if (err) {
      const detail = JSON.stringify(err).match(/"value":"([^"]+)"/);
      bad.push(`${n}: ${detail ? detail[1] : JSON.stringify(err).slice(0, 200)}`);
    }
  }
  console.log(`checked ${names.length}, errors ${bad.length}`);
  bad.forEach((b) => console.log("  " + b));
})().catch((e) => { console.error(e); process.exit(1); });
