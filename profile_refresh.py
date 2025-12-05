"""
Profile the refresh_old_games script to identify performance bottlenecks.
"""
import cProfile
import pstats
import io
from pstats import SortKey
import time

def main():
    print("Starting profiling of refresh_old_games...")
    print("=" * 80)

    # Start timing
    start_time = time.time()

    # Profile the script
    profiler = cProfile.Profile()
    profiler.enable()

    # Import and run the script
    from src.pipeline import refresh_old_games
    refresh_old_games.main()

    profiler.disable()

    # End timing
    end_time = time.time()
    total_time = end_time - start_time

    print("=" * 80)
    print(f"\nTotal execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print("\n" + "=" * 80)
    print("PROFILING RESULTS - Top 30 functions by cumulative time")
    print("=" * 80)

    # Print stats
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.strip_dirs()
    ps.sort_stats(SortKey.CUMULATIVE)
    ps.print_stats(30)
    print(s.getvalue())

    print("\n" + "=" * 80)
    print("PROFILING RESULTS - Top 30 functions by total time")
    print("=" * 80)

    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.strip_dirs()
    ps.sort_stats(SortKey.TIME)
    ps.print_stats(30)
    print(s.getvalue())

    # Save detailed profile to file
    profiler.dump_stats('profile_output.prof')
    print("\nDetailed profile saved to: profile_output.prof")
    print("You can analyze it with: python -m pstats profile_output.prof")

if __name__ == "__main__":
    main()