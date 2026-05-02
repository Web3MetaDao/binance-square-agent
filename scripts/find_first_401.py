import subprocess
r = subprocess.run(
    ["grep", "-n", "401", "/root/binance-square-agent/data/pipeline_loop.log"],
    capture_output=True, text=True
)
lines = r.stdout.strip().split('\n')
if lines:
    first = lines[0]
    line_num = int(first.split(':')[0])
    r2 = subprocess.run(
        ["sed", f"{line_num-5},{line_num+3}p", "-n", "/root/binance-square-agent/data/pipeline_loop.log"],
        capture_output=True, text=True
    )
    print(r2.stdout)
