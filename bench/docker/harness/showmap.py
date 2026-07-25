import json, sys, urllib.request, re
prefix = "d1"
maps = []
xml = urllib.request.urlopen(f"http://127.0.0.1:9500/ladder/?list-type=2&prefix={prefix}/segmaps/", timeout=15).read().decode()
for k in re.findall(r"<Key>([^<]+)</Key>", xml):
    m = json.loads(urllib.request.urlopen(f"http://127.0.0.1:9500/ladder/{k}", timeout=15).read())
    live = [s for s in m["segments"] if not s.get("sealed_ms")]
    sealed = [s for s in m["segments"] if s.get("sealed_ms")]
    maps.append((k.split("/")[-1][:10], m["version"], len(live), len(sealed)))
for h, v, l, s in maps:
    print(f"{h} v{v} live={l} sealed={s}")
