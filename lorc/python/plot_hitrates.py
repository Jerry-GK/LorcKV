import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_csv_files():
    # Use more generic font settings
    plt.rcParams['font.family'] = ['DejaVu Sans', 'sans-serif']
    
    # Create chart
    plt.figure(figsize=(12, 6))
    
    # Read and plot each CSV file
    csv_files = ['../output/hitrates_victim_oldest.csv', 
                 '../output/hitrates_victim_random.csv', 
                 '../output/hitrates_victim_shortest.csv',
                 '../output/hitrates_victim_longest.csv',
                 '../output/hitrates_victim_lru.csv']
    labels = ['Victim Oldest', 'Victim Random', 'Victim Shortest', 'Victim Longest', 'Victim LRU']
    colors = ['red', 'blue', 'green', 'purple', 'orange']
    
    for file, label, color in zip(csv_files, labels, colors):
        if os.path.exists(file):
            df = pd.read_csv(file, header=None, names=['query_num', 'hit_rate'])
            line = plt.plot(df['query_num'], df['hit_rate'] * 100,  # Convert to percentage
                    label=label, color=color, linewidth=1)
            
            # Add annotation at the last point
            last_x = df['query_num'].iloc[-1]
            last_y = df['hit_rate'].iloc[-1] * 100
            plt.annotate(f'{last_y:.1f}%', 
                        xy=(last_x, last_y),
                        xytext=(10, 0),
                        textcoords='offset points',
                        fontsize=8,
                        color=color)
    
    # Set chart properties (in English)
    plt.title('Cache Hit Rate Comparison\nQuery Range Size Ratio = 2% (Random)\nMemory Ratio = 50%\nHit: Full Range Contained', fontsize=14, pad=20)
    plt.xlabel('Query Num', fontsize=12)
    plt.ylabel('Hit Rate (%)', fontsize=12)  # Modify y-axis label
    plt.grid(True, linestyle='--', alpha=0.5, linewidth=0.5)
    plt.legend(fontsize=10)
    
    # Set y-axis ticks to percentage format
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.0f}%'.format(y)))

    # Save chart
    plt.savefig('../output/cache_comparison.png', dpi=300, bbox_inches='tight')
    print("Chart saved to ../output/cache_comparison.png")

if __name__ == "__main__":
    plot_csv_files()
