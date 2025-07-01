"""Analysis module for Upstart job posting data visualization.

This module provides functions to create various visualizations and analyses
of the job posting data, focusing on hiring trends and departmental insights.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
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

def extract_position_level(title: str) -> str:
    """Extract position/seniority level from job title.
    
    Args:
        title: Job title string
        
    Returns:
        Position level (e.g., 'Associate', 'Senior', 'Manager', etc.)
    """
    if pd.isna(title):
        return 'Unknown'
    
    title_lower = title.lower()
    
    # Define position hierarchy in order of seniority
    position_patterns = [
        ('VP', r'\bvp\b|\bvice president\b'),
        ('Director', r'\bdirector\b'),
        ('Principal', r'\bprincipal\b'),
        ('Manager', r'\bmanager\b'),
        ('Senior', r'\bsenior\b|\bsr\b'),
        ('Staff', r'\bstaff\b'),
        ('Lead', r'\blead\b'),
        ('Associate', r'\bassociate\b'),
        ('Junior', r'\bjunior\b|\bjr\b'),
        ('Intern', r'\bintern\b'),
    ]
    
    for level, pattern in position_patterns:
        if re.search(pattern, title_lower):
            return level
    
    return 'Individual Contributor'


def plot_monthly_positions_by_department(df: pd.DataFrame, 
                                       figsize: tuple = (12, 8),
                                       title: Optional[str] = None) -> plt.Figure:
    """Create a stacked bar chart showing position counts by department for each month.
    
    Args:
        df: DataFrame with columns 'first_published', 'departments', and 'title'
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
    
    # Extract position level from title
    df_work['position_level'] = df_work['title'].apply(extract_position_level)
    
    # Clean up department names (handle potential null values)
    df_work['departments'] = df_work['departments'].fillna('Unknown')
    
    # Create pivot table: rows=month_year, columns=position_level, values=count by department
    # First, we need to aggregate by month, department, and position level
    monthly_dept_pos = df_work.groupby(['month_year', 'departments', 'position_level']).size().reset_index(name='count')
    
    # Get unique months and departments for consistent ordering
    months = sorted(monthly_dept_pos['month_year'].unique())
    departments = sorted(monthly_dept_pos['departments'].unique())
    position_levels = sorted(monthly_dept_pos['position_level'].unique(), 
                           key=lambda x: ['VP', 'Director', 'Principal', 'Manager', 'Senior', 
                                        'Staff', 'Lead', 'Associate', 'Junior', 'Intern', 
                                        'Individual Contributor', 'Unknown'].index(x) 
                                        if x in ['VP', 'Director', 'Principal', 'Manager', 'Senior', 
                                               'Staff', 'Lead', 'Associate', 'Junior', 'Intern', 
                                               'Individual Contributor', 'Unknown'] else 999)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create a color palette for position levels
    colors = plt.cm.Set3(range(len(position_levels)))
    color_map = dict(zip(position_levels, colors))
    
    # Prepare data for stacked bar chart
    month_labels = [str(m) for m in months]
    x_pos = range(len(months))
    
    # For each department, create a grouped bar
    bar_width = 0.8 / len(departments) if departments else 0.8
    
    for dept_idx, department in enumerate(departments):
        dept_data = monthly_dept_pos[monthly_dept_pos['departments'] == department]
        
        # Create bottom array for stacking
        bottoms = [0] * len(months)
        
        for pos_level in position_levels:
            counts = []
            for month in months:
                count = dept_data[
                    (dept_data['month_year'] == month) & 
                    (dept_data['position_level'] == pos_level)
                ]['count'].sum()
                counts.append(count)
            
            # Calculate x positions for this department
            x_positions = [x + (dept_idx - len(departments)/2 + 0.5) * bar_width for x in x_pos]
            
            # Create stacked bars
            bars = ax.bar(x_positions, counts, bar_width, 
                         bottom=bottoms, label=f'{pos_level}' if dept_idx == 0 else "",
                         color=color_map[pos_level], alpha=0.8)
            
            # Update bottoms for next stack level
            bottoms = [b + c for b, c in zip(bottoms, counts)]
    
    # Customize the plot
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Number of Positions', fontsize=12)
    
    if title is None:
        title = 'Job Postings by Department and Position Level (Monthly)'
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Set x-axis labels
    ax.set_xticks(x_pos)
    ax.set_xticklabels(month_labels, rotation=45, ha='right')
    
    # Add legend for position levels
    ax.legend(title='Position Level', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Add department labels
    if len(departments) > 1:
        # Create second legend for departments
        dept_handles = []
        dept_colors = plt.cm.tab10(range(len(departments)))
        
        for dept_idx, department in enumerate(departments):
            x_center = (dept_idx - len(departments)/2 + 0.5) * bar_width
            ax.text(x_center, -0.02, department, transform=ax.get_xaxis_transform(), 
                   ha='center', va='top', rotation=45, fontsize=10)
    
    # Improve layout
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    return fig


def plot_monthly_departments_by_position(df: pd.DataFrame,
                                        figsize: tuple = (14, 8),
                                        title: Optional[str] = None) -> plt.Figure:
    """Create a stacked bar chart showing departments by position level for each month.
    
    Args:
        df: DataFrame with columns 'first_published', 'departments', and 'title'
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
    
    # Extract position level from title
    df_work['position_level'] = df_work['title'].apply(extract_position_level)
    
    # Clean up department names (handle potential null values)
    df_work['departments'] = df_work['departments'].fillna('Unknown')
    
    # Group by month, position level, and department
    monthly_data = df_work.groupby(['month_year', 'position_level', 'departments']).size().reset_index(name='count')
    
    # Create pivot table: rows=month_year, columns=departments, values=count for each position
    pivot_data = monthly_data.pivot_table(
        index='month_year', 
        columns=['position_level', 'departments'], 
        values='count', 
        fill_value=0
    )
    
    # Flatten the multi-level columns for easier handling
    pivot_data.columns = [f'{pos}_{dept}' for pos, dept in pivot_data.columns]
    
    # Create the plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Get unique departments and position levels for color mapping
    departments = sorted(df_work['departments'].unique())
    position_levels = ['VP', 'Director', 'Principal', 'Manager', 'Senior', 
                      'Staff', 'Lead', 'Associate', 'Junior', 'Intern', 
                      'Individual Contributor', 'Unknown']
    
    # Create color palettes
    dept_colors = plt.cm.tab20(range(len(departments)))
    pos_patterns = ['', '////', '....', '----', '||||', '\\\\\\\\', '+++', 'xxx']
    
    # Create stacked bars
    bottom = [0] * len(pivot_data.index)
    
    for i, pos_level in enumerate(position_levels):
        if any(col.startswith(pos_level + '_') for col in pivot_data.columns):
            for j, dept in enumerate(departments):
                col_name = f'{pos_level}_{dept}'
                if col_name in pivot_data.columns:
                    values = pivot_data[col_name].values
                    
                    # Only plot if there are non-zero values
                    if values.sum() > 0:
                        pattern = pos_patterns[i % len(pos_patterns)]
                        bars = ax.bar(
                            range(len(pivot_data.index)), 
                            values, 
                            bottom=bottom,
                            label=f'{pos_level} - {dept}',
                            color=dept_colors[j],
                            alpha=0.8,
                            hatch=pattern
                        )
                        
                        # Update bottom for stacking
                        bottom = [b + v for b, v in zip(bottom, values)]
    
    # Customize the plot
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Number of Positions', fontsize=12)
    
    if title is None:
        title = 'Job Postings by Position Level and Department (Monthly)'
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Set x-axis labels
    ax.set_xticks(range(len(pivot_data.index)))
    ax.set_xticklabels([str(idx) for idx in pivot_data.index], rotation=45, ha='right')
    
    # Add legend (this might be crowded, so we'll put it outside)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    
    # Add grid
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    return fig


def plot_monthly_positions_by_department_clean(df: pd.DataFrame,
                                             figsize: tuple = (14, 8),
                                             title: Optional[str] = None) -> plt.Figure:
    """Create a stacked bar chart showing position counts by department for each month.
    
    This is the main function requested: for each month on x-axis, show stacked bars
    where each stack represents different departments, and the height represents
    the count of positions.
    
    Args:
        df: DataFrame with columns 'first_published', 'departments', and 'title'
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
    
    # Extract position level from title
    df_work['position_level'] = df_work['title'].apply(extract_position_level)
    
    # Clean up department names (handle potential null values)
    df_work['departments'] = df_work['departments'].fillna('Unknown')
    
    # Map departments to combined categories using the DEPARTMENT_MAPPING
    df_work['combined_departments'] = df_work['departments'].map(DEPARTMENT_MAPPING).fillna('Other')
    
    # Group by month and combined department to get counts
    monthly_dept_counts = df_work.groupby(['month_year', 'combined_departments']).size().reset_index(name='count')
    
    # Pivot to get departments as columns
    pivot_data = monthly_dept_counts.pivot(index='month_year', columns='combined_departments', values='count').fillna(0)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create stacked bar chart with different colors for each department
    colors = plt.cm.tab20(range(len(pivot_data.columns)))
    pivot_data.plot(kind='bar', stacked=True, ax=ax, color=colors, alpha=0.8, width=0.8)
    
    # Customize the plot
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Number of Positions', fontsize=12)
    
    if title is None:
        title = 'Job Postings by Department (Monthly Stacked)'
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Format x-axis labels
    ax.set_xticklabels([str(idx) for idx in pivot_data.index], rotation=45, ha='right')
    
    # Add legend
    ax.legend(title='Department', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    # Add grid for better readability
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    # Add value labels on bars if there aren't too many months
    if len(pivot_data.index) <= 12:
        for i, month in enumerate(pivot_data.index):
            cumulative = 0
            for j, dept in enumerate(pivot_data.columns):
                value = pivot_data.loc[month, dept]
                if value > 0:  # Only label non-zero values
                    ax.text(i, cumulative + value/2, f'{int(value)}', 
                           ha='center', va='center', fontsize=8, fontweight='bold')
                    cumulative += value
    
    return fig


def plot_simplified_monthly_positions(df: pd.DataFrame, 
                                    figsize: tuple = (12, 6),
                                    title: Optional[str] = None) -> plt.Figure:
    """Create a simplified stacked bar chart showing position levels by month (all departments combined).
    
    Args:
        df: DataFrame with columns 'first_published', 'departments', and 'title'
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
    
    # Extract position level from title
    df_work['position_level'] = df_work['title'].apply(extract_position_level)
    
    # Group by month and position level
    monthly_pos = df_work.groupby(['month_year', 'position_level']).size().reset_index(name='count')
    
    # Pivot to get position levels as columns
    pivot_data = monthly_pos.pivot(index='month_year', columns='position_level', values='count').fillna(0)
    
    # Sort columns by seniority
    position_order = ['VP', 'Director', 'Principal', 'Manager', 'Senior', 
                     'Staff', 'Lead', 'Associate', 'Junior', 'Intern', 
                     'Individual Contributor', 'Unknown']
    
    # Reorder columns based on available position levels
    available_positions = [pos for pos in position_order if pos in pivot_data.columns]
    other_positions = [pos for pos in pivot_data.columns if pos not in position_order]
    column_order = available_positions + other_positions
    
    pivot_data = pivot_data[column_order]
    
    # Create the plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create stacked bar chart
    colors = plt.cm.Set3(range(len(pivot_data.columns)))
    pivot_data.plot(kind='bar', stacked=True, ax=ax, color=colors, alpha=0.8)
    
    # Customize the plot
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Number of Positions', fontsize=12)
    
    if title is None:
        title = 'Job Postings by Position Level (Monthly - All Departments)'
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Format x-axis labels
    ax.set_xticklabels([str(idx) for idx in pivot_data.index], rotation=45, ha='right')
    
    # Add legend
    ax.legend(title='Position Level', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Add grid
    plt.grid(axis='y', alpha=0.3)
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
        
        # Create the main requested plot: positions by department for each month
        fig = plot_monthly_positions_by_department_clean(df)
        plt.show()
        
        # Also create the simplified position-level plot
        # fig2 = plot_simplified_monthly_positions(df)
        # plt.show()
    else:
        print("No data available to plot") 