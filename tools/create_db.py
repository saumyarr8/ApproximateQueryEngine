from aqe_backend import run_query, run_query_groupby
import time

def show(title, f):
    print("----", title, "----")
    start = time.time()
    r = f()
    elapsed = time.time() - start
    print("Result:", r)
    print("Time: {:.3f}s".format(elapsed))
    print()

def main():
    show("Exact COUNT", lambda: run_query("SELECT COUNT(id) FROM sales"))
    show("Approx COUNT 10%", lambda: run_query("SELECT COUNT(id) FROM sales", sample_percent=10))
    show("Exact AVG WHERE", lambda: run_query("SELECT AVG(amount) FROM sales WHERE category='A'"))
    show("Approx AVG WHERE 20%", lambda: run_query("SELECT AVG(amount) FROM sales WHERE category='A'", sample_percent=20))

    print("GROUP BY (SUM) approx 20% using 4 threads:")
    res = run_query_groupby("SELECT SUM(amount) FROM sales GROUP BY category", sample_percent=20, num_threads=4)
    print(res)

if __name__ == "__main__":
    main()