import subprocess
import matplotlib.pyplot as plt
from upst import load_clean
from analysis import plot_monthly_positions_by_department_clean, get_recent_postings, get_recent_urls

def main():
    print("Running scraper...")
    try:
        subprocess.run(["python3", "-m", "upst.scraper"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Scraper failed with error: {e}")
        print("Attempting to proceed with existing data...")

    print("Loading data...")
    # Notebook used load_clean(latest_only=True)
    df = load_clean(latest_only=True)
    
    if df.empty:
        print("No data found.")
        return

    print("Generating plot...")
    fig = plot_monthly_positions_by_department_clean(df)
    
    plot_filename = "monthly_positions_by_department.png"
    fig.savefig(plot_filename)
    print(f"Plot saved to {plot_filename}")
    
    print("Getting recent postings...")
    # Notebook logic
    recent_posts = get_recent_postings(df, days_back=30)
    print(f"Found {len(recent_posts)} recent postings")
    
    # Optimization: define recent_urls from recent_posts directly to ensure alignment and avoid re-computation
    # But sticking to notebook logic structure ensures same behavior
    recent_urls = get_recent_urls(df, days_back=30)
    
    links_filename = "recent_links.md"
    with open(links_filename, 'w') as f:
        f.write("# Recent Job Postings\n\n")
        for i, url in enumerate(recent_urls):
            # Notebook format: print(f"title: {recent_posts.iloc[i]['title']}. {url}")
            title = recent_posts.iloc[i]['title']
            line = f"title: {title}. {url}"
            print(line)
            f.write(f"- [{title}]({url})\n")
            
    print(f"Links saved to {links_filename}")

if __name__ == "__main__":
    main()
