import os

# Change 'script1.py' and 'script2.py' to the names of your Python files
scripts = ['get_news_links.py', 'get_news_content.py']

for script in scripts:
    print(f"Running {script}...")
    os.system(f"python {script}")
    print(f"{script} finished.")
