"""Analysis module for Upstart job posting data visualization.

This module provides functions to create various visualizations and analyses
of the job posting data, focusing on hiring trends and departmental insights.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import re
from typing import Optional

DEPARTMENT_MAPPING = {
    "Capital Markets":       "Finance & Capital Markets",
    "Compliance":            "Legal & Compliance",
    "Consumer Marketing":    "Marketing & Growth",
    "Data Analytics":        "Data",
    "Data Engineering":      "Data",
    "Engineering":           "Engineering",
    "Finance":               "Finance & Capital Markets",
    "Growth":                "Marketing & Growth",
    "IT":                    "IT & Security",
    "InfoSec":               "IT & Security",
    "Legal":                 "Legal & Compliance",
    "Machine Learning":      "AI",
    "Onboarding":            "Operations",
    "Operations":            "Operations",
    "Partner Operations":    "Operations",
    "People":                "People (HR)",
    "Product":               "Product",
    "Product Design":        "Product",
    "Product Management":    "Product",
    "Research Scientist":    "AI",
    "Software Development":  "Engineering"
}


def get_recent_postings(df: pd.DataFrame, days_back: int = 30) -> pd.DataFrame:
    """Get job postings with first_published date within the last N days.
    
    Args:
        df: DataFrame with columns 'first_published' and 'absolute_url'
        days_back: Number of days to look back from today (default: 30)
        
    Returns:
        DataFrame containing only the recent postings with absolute_url, title, 
        first_published, and departments columns
    """
    # Create a copy to avoid modifying the original dataframe
    df_work = df.copy()
    
    # Convert first_published to datetime with UTC timezone handling
    df_work['first_published'] = pd.to_datetime(df_work['first_published'], utc=True)
    
    # Calculate the cutoff date (N days ago from today)
    cutoff_date = datetime.now().replace(tzinfo=None) - timedelta(days=days_back)
    
    # Convert cutoff_date to timezone-aware if needed
    if df_work['first_published'].dt.tz is not None:
        from datetime import timezone
        cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
    else:
        # Remove timezone info from first_published if cutoff_date is naive
        df_work['first_published'] = df_work['first_published'].dt.tz_localize(None)
    
    # Filter for recent postings
    recent_mask = df_work['first_published'] >= cutoff_date
    recent_df = df_work[recent_mask].copy()
    
    # Select relevant columns and sort by first_published (newest first)
    columns_to_show = ['absolute_url', 'title', 'first_published', 'departments']
    available_columns = [col for col in columns_to_show if col in recent_df.columns]
    
    result = recent_df[available_columns].sort_values('first_published', ascending=False)
    
    return result


def get_recent_urls(df: pd.DataFrame, days_back: int = 30) -> list[str]:
    """Get a simple list of URLs for job postings published in the last N days.
    
    Args:
        df: DataFrame with columns 'first_published' and 'absolute_url'
        days_back: Number of days to look back from today (default: 30)
        
    Returns:
        List of absolute URLs for recent job postings, sorted by publish date (newest first)
    """
    recent_df = get_recent_postings(df, days_back=days_back)
    return recent_df['absolute_url'].tolist()


def plot_monthly_positions_by_department_clean(df: pd.DataFrame,
                                             figsize: tuple = (18, 8),
                                             title: Optional[str] = None) -> plt.Figure:
    """Create a stacked bar chart showing position counts by department for each month,
    along with a pie chart showing overall department composition.
    
    This is the main function requested: for each month on x-axis, show stacked bars
    where each stack represents different departments, and the height represents
    the count of positions. Additionally shows a pie chart with overall composition.
    
    Args:
        df: DataFrame with columns 'first_published' and 'departments'
        figsize: Figure size as (width, height)
        title: Optional custom title for the plot
        
    Returns:
        matplotlib Figure object
    """
    # Create a copy to avoid modifying the original dataframe
    df_work = df.copy()
    
    # Convert first_published to datetime with UTC timezone handling
    df_work['first_published'] = pd.to_datetime(df_work['first_published'], utc=True)
    
    # Extract month-year from first_published
    df_work['month_year'] = df_work['first_published'].dt.to_period('M')
    
    # Clean up department names (handle potential null values)
    df_work['departments'] = df_work['departments'].fillna('Unknown')
    
    # Map departments to combined categories using the DEPARTMENT_MAPPING
    df_work['combined_departments'] = df_work['departments'].map(DEPARTMENT_MAPPING).fillna('Other')
    
    # Group by month and combined department to get counts
    monthly_dept_counts = df_work.groupby(['month_year', 'combined_departments']).size().reset_index(name='count')
    
    # Pivot to get departments as columns
    pivot_data = monthly_dept_counts.pivot(index='month_year', columns='combined_departments', values='count').fillna(0)
    
    # Calculate overall department composition for pie chart
    overall_dept_counts = df_work['combined_departments'].value_counts()
    
    # Create subplot layout: bar chart on left, pie chart on right
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize, gridspec_kw={'width_ratios': [2, 1]})
    
    # Create stacked bar chart with different colors for each department
    colors = plt.cm.tab20(range(len(pivot_data.columns)))
    pivot_data.plot(kind='bar', stacked=True, ax=ax1, color=colors, alpha=0.8, width=0.8)
    
    # Customize the bar chart
    ax1.set_xlabel('Month', fontsize=12)
    ax1.set_ylabel('Number of Positions', fontsize=12)
    
    if title is None:
        title = 'Job Postings by Department (Monthly Stacked)'
    ax1.set_title(title, fontsize=14, fontweight='bold')
    
    # Format x-axis labels
    ax1.set_xticklabels([str(idx) for idx in pivot_data.index], rotation=45, ha='right')
    
    # Add legend for bar chart
    ax1.legend(title='Department', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    # Add grid for better readability
    ax1.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars if there aren't too many months
    if len(pivot_data.index) <= 12:
        for i, month in enumerate(pivot_data.index):
            cumulative = 0
            for j, dept in enumerate(pivot_data.columns):
                value = pivot_data.loc[month, dept]
                if value > 0:  # Only label non-zero values
                    ax1.text(i, cumulative + value/2, f'{int(value)}', 
                           ha='center', va='center', fontsize=8, fontweight='bold')
                    cumulative += value
    
    # Create pie chart showing overall composition
    # Use same colors as bar chart for consistency
    dept_colors = {}
    for i, dept in enumerate(pivot_data.columns):
        dept_colors[dept] = colors[i]
    
    pie_colors = [dept_colors.get(dept, '#cccccc') for dept in overall_dept_counts.index]
    
    wedges, texts, autotexts = ax2.pie(overall_dept_counts.values, 
                                       labels=overall_dept_counts.index,
                                       autopct='%1.1f%%',
                                       colors=pie_colors,
                                       startangle=90)
    
    # Customize pie chart
    ax2.set_title('Overall Department Composition\n(All Time)', fontsize=12, fontweight='bold')
    
    # Make percentage text more readable
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(9)
    
    # Adjust label text size
    for text in texts:
        text.set_fontsize(9)
    
    plt.tight_layout()
    
    return fig


if __name__ == "__main__":
    # Example usage
    from upst import load_clean
    
    # Load the data
    df = load_clean()
    
    if not df.empty:
        print(f"Loaded {len(df)} job postings")
        print(f"Date range: {df['first_published'].min()} to {df['first_published'].max()}")
        
        # Show recent postings (last 30 days)
        recent_postings = get_recent_postings(df, days_back=30)
        recent_urls = get_recent_urls(df, days_back=30)
        
        print(f"\n--- Recent Job Postings (Last 30 Days) ---")
        print(f"Found {len(recent_postings)} recent postings:")
        
        if not recent_postings.empty:
            # Display each recent posting
            for idx, row in recent_postings.iterrows():
                print(f"\nTitle: {row['title']}")
                print(f"Department: {row.get('departments', 'N/A')}")
                print(f"Published: {row['first_published']}")
                print(f"URL: {row['absolute_url']}")
            
            print(f"\n--- Recent URLs Only ---")
            print("For programmatic access, use get_recent_urls():")
            for i, url in enumerate(recent_urls[:5], 1):  # Show first 5 as example
                print(f"{i}. {url}")
            if len(recent_urls) > 5:
                print(f"... and {len(recent_urls) - 5} more")
        else:
            print("No job postings found in the last 30 days.")
        
        # Create the main requested plot: positions by department for each month
        fig = plot_monthly_positions_by_department_clean(df)
        plt.show()
    else:
        print("No data available to plot") 