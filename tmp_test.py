#!/usr/bin/env python3

# Test the improved HTML cleaning function
from upst import load_clean
from upst.clean import _clean_content

# Test with challenging HTML content
test_cases = [
    # Basic HTML with entities
    "&lt;p&gt;Hello &amp; world&lt;/p&gt;",
    
    # Complex nested HTML
    "&lt;div&gt;&lt;p&gt;&lt;strong&gt;Bold&lt;/strong&gt; and &lt;em&gt;italic&lt;/em&gt;&lt;/p&gt;&lt;/div&gt;",
    
    # HTML with extra whitespace and special entities
    "&lt;p&gt;Multiple&nbsp;&nbsp;&nbsp;spaces   and\n\nnewlines&lt;/p&gt;",
    
    # Real content from dataframe
    None  # Will be filled with actual data
]

# Load actual data
df = load_clean()
test_cases[3] = df.content[0]

print("Testing improved _clean_content function:")
print("=" * 60)

for i, test_html in enumerate(test_cases):
    print(f"\nTest case {i+1}:")
    if i == 3:
        print("Original (first 200 chars):", test_html[:200] + "...")
    else:
        print("Original:", test_html)
    
    try:
        cleaned = _clean_content(test_html)
        if i == 3:
            print("Cleaned (first 300 chars):", cleaned[:300] + "...")
            print(f"Length reduction: {len(test_html)} → {len(cleaned)} ({100*(len(test_html)-len(cleaned))/len(test_html):.1f}% smaller)")
        else:
            print("Cleaned:", cleaned)
    except Exception as e:
        print(f"Error: {e}")

# Test the main cleaning pipeline
print(f"\n{'='*60}")
print("Testing full cleaning pipeline:")

from upst.clean import clean_nested_columns
import pandas as pd

# Create test dataframe with problematic content
test_df = pd.DataFrame([
    {'content': "&lt;div&gt;&lt;p&gt;Test&nbsp;content&lt;/p&gt;&lt;/div&gt;", 'job_id': 'test1'},
    {'content': df.content[0], 'job_id': 'test2'}
])

print("Before cleaning:")
for i, content in enumerate(test_df.content):
    preview = content[:100] + "..." if len(content) > 100 else content
    print(f"  Row {i}: {preview}")

cleaned_df = clean_nested_columns(test_df)

print("\nAfter cleaning:")
for i, content in enumerate(cleaned_df.content):
    preview = content[:100] + "..." if len(content) > 100 else content
    print(f"  Row {i}: {preview}")

# Test the newly scraped data with improved cleaning
from upst import load_clean

# Load the data (should now include the newly scraped clean data)
df = load_clean()

print(f"Loaded dataframe with {len(df)} job postings")
print(f"Columns: {list(df.columns)}")

# Check content of the first few jobs
print("\n" + "="*60)
print("Sample of cleaned content:")

for i in range(min(3, len(df))):
    content = df.content.iloc[i]
    print(f"\nJob {i+1} - {df.title.iloc[i]}:")
    print(f"Content length: {len(content)} characters")
    print(f"Content preview: {content[:200]}...")
    
    # Check if there are any HTML tags left (there shouldn't be)
    import re
    html_tags = re.findall(r'<[^>]+>', content)
    html_entities = re.findall(r'&[a-zA-Z0-9#]+;', content)
    
    print(f"HTML tags found: {len(html_tags)} {'(✅ Clean)' if len(html_tags) == 0 else '(❌ Still has tags)'}")
    print(f"HTML entities found: {len(html_entities)} {'(✅ Clean)' if len(html_entities) == 0 else '(❌ Still has entities)'}")
    
    if html_tags:
        print(f"  Sample tags: {html_tags[:3]}")
    if html_entities:
        print(f"  Sample entities: {html_entities[:3]}")

# Check overall data quality
print(f"\n{'='*60}")
print("Overall data quality check:")

total_jobs = len(df)
jobs_with_content = df.content.notna().sum()
avg_content_length = df.content.str.len().mean()

print(f"Total jobs: {total_jobs}")
print(f"Jobs with content: {jobs_with_content}")
print(f"Average content length: {avg_content_length:.0f} characters")

# Check for any remaining HTML across all content
all_content = ' '.join(df.content.fillna(''))
remaining_tags = len(re.findall(r'<[^>]+>', all_content))
remaining_entities = len(re.findall(r'&[a-zA-Z0-9#]+;', all_content))

print(f"Total HTML tags remaining: {remaining_tags} {'(✅ All clean)' if remaining_tags == 0 else '(❌ Still has tags)'}")
print(f"Total HTML entities remaining: {remaining_entities} {'(✅ All clean)' if remaining_entities == 0 else '(❌ Still has entities)'}")
