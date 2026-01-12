"""Demo script for testing status tracking via text parsing.

Run with:
    run job -k python -m runtools.runcli.demo_status

The -k flag enables KV parsing to extract status fields from output.
"""
import time


def main():
    # Initial events before download
    print("event=[init]", flush=True)
    time.sleep(0.3)
    print("event=[connecting]", flush=True)
    time.sleep(0.3)
    print("event=[authenticated]", flush=True)
    time.sleep(0.3)

    # Simulate download with progress
    total_files = 5
    for i in range(1, total_files + 1):
        print(f"event=[downloading] completed=[{i}] total=[{total_files}] unit=[files]", flush=True)
        time.sleep(0.4)

    # Simulate processing
    print("event=[processing]", flush=True)
    time.sleep(0.3)

    # Final result
    print("result=[success]", flush=True)


if __name__ == '__main__':
    main()
