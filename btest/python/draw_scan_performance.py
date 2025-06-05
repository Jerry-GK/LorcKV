#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Control variable 
with_update = True
enable_dio = True
drop_outliers = False

# Generate file paths based on with_update variable
update_suffix = "_update" if with_update else "_noupdate"
dio_suffix = "_dio" if enable_dio else "_nodio"
outliers_suffix = "_drop_outliers" if drop_outliers else ""
rocksdb_lorc_csv_path = f'./csv/scan_performance_rocksdb_lorc{update_suffix}{dio_suffix}.csv'
rocksdb_blockcache_csv_path = f'./csv/scan_performance_rocksdb_blockcache{update_suffix}{dio_suffix}.csv' # Corrected path
blobdb_lorc_csv_path = f'./csv/scan_performance_blobdb_lorc{update_suffix}{dio_suffix}.csv'
blobdb_blobcache_csv_path = f'./csv/scan_performance_blobdb_blobcache{update_suffix}{dio_suffix}.csv'
output_image_path = f'./scan_performance_comparison{update_suffix}{dio_suffix}{outliers_suffix}.png'

# Read CSV files
df_rlorc_orig = pd.read_csv(rocksdb_lorc_csv_path)
df_rbc_orig = pd.read_csv(rocksdb_blockcache_csv_path)
df_blorc_orig = pd.read_csv(blobdb_lorc_csv_path)
df_bbc_orig = pd.read_csv(blobdb_blobcache_csv_path)

# --- Initial Data analysis for Outlier Detection (on original data) ---
# RocksDB Lorc
mean_rlorc_orig = df_rlorc_orig['ScanTime(s)'].mean()
std_rlorc_orig = df_rlorc_orig['ScanTime(s)'].std()
threshold_rlorc = mean_rlorc_orig + 2 * std_rlorc_orig
outliers_rlorc_indices = df_rlorc_orig[df_rlorc_orig['ScanTime(s)'] > threshold_rlorc].index
num_outliers_rlorc = len(outliers_rlorc_indices)

# RocksDB BlockCache
mean_rbc_orig = df_rbc_orig['ScanTime(s)'].mean()
std_rbc_orig = df_rbc_orig['ScanTime(s)'].std()
threshold_rbc = mean_rbc_orig + 2 * std_rbc_orig
outliers_rbc_indices = df_rbc_orig[df_rbc_orig['ScanTime(s)'] > threshold_rbc].index
num_outliers_rbc = len(outliers_rbc_indices)

# BlobDB Lorc
mean_blorc_orig = df_blorc_orig['ScanTime(s)'].mean()
std_blorc_orig = df_blorc_orig['ScanTime(s)'].std()
threshold_blorc = mean_blorc_orig + 2 * std_blorc_orig
outliers_blorc_indices = df_blorc_orig[df_blorc_orig['ScanTime(s)'] > threshold_blorc].index
num_outliers_blorc = len(outliers_blorc_indices)

# BlobDB BlobCache
mean_bbc_orig = df_bbc_orig['ScanTime(s)'].mean()
std_bbc_orig = df_bbc_orig['ScanTime(s)'].std()
threshold_bbc = mean_bbc_orig + 2 * std_bbc_orig
outliers_bbc_indices = df_bbc_orig[df_bbc_orig['ScanTime(s)'] > threshold_bbc].index
num_outliers_bbc = len(outliers_bbc_indices)


# --- Conditionally Drop Outliers ---
if drop_outliers:
    df_rlorc = df_rlorc_orig.drop(outliers_rlorc_indices)
    df_rbc = df_rbc_orig.drop(outliers_rbc_indices)
    df_blorc = df_blorc_orig.drop(outliers_blorc_indices)
    df_bbc = df_bbc_orig.drop(outliers_bbc_indices)
    outliers_rlorc_plot = pd.DataFrame()
    outliers_rbc_plot = pd.DataFrame()
    outliers_blorc_plot = pd.DataFrame()
    outliers_bbc_plot = pd.DataFrame()
else:
    df_rlorc = df_rlorc_orig.copy()
    df_rbc = df_rbc_orig.copy()
    df_blorc = df_blorc_orig.copy()
    df_bbc = df_bbc_orig.copy()
    outliers_rlorc_plot = df_rlorc_orig.loc[outliers_rlorc_indices]
    outliers_rbc_plot = df_rbc_orig.loc[outliers_rbc_indices]
    outliers_blorc_plot = df_blorc_orig.loc[outliers_blorc_indices]
    outliers_bbc_plot = df_bbc_orig.loc[outliers_bbc_indices]

# Data analysis (on potentially filtered data)
mean_rlorc = df_rlorc['ScanTime(s)'].mean()
std_rlorc = df_rlorc['ScanTime(s)'].std()
mean_rbc = df_rbc['ScanTime(s)'].mean()
std_rbc = df_rbc['ScanTime(s)'].std()
mean_blorc = df_blorc['ScanTime(s)'].mean()
std_blorc = df_blorc['ScanTime(s)'].std()
mean_bbc = df_bbc['ScanTime(s)'].mean()
std_bbc = df_bbc['ScanTime(s)'].std()

# Create figure with subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), gridspec_kw={'height_ratios': [2, 1]})

# Plot 1: Log scale plot for all datasets
ax1.plot(df_rbc['Query'], df_rbc['ScanTime(s)'], linestyle='-',
         color='darkgreen', linewidth=0.8, label='RocksDB BlockCache')
ax1.plot(df_rlorc['Query'], df_rlorc['ScanTime(s)'], linestyle='-',
         color='lightgreen', linewidth=0.8, label='RocksDB Lorc')
ax1.plot(df_bbc['Query'], df_bbc['ScanTime(s)'], linestyle='-',
         color='darkblue', linewidth=0.8, label='BlobDB BlobCache')
ax1.plot(df_blorc['Query'], df_blorc['ScanTime(s)'], linestyle='-',
         color='lightblue', linewidth=0.8, label='BlobDB Lorc')
ax1.set_ylabel('Scan Time (seconds) - Log Scale')
ax1.set_xlabel('Operation Number')
ax1.set_title('Scan Performance Comparison (20%Write + 80%Scan, Enough Memory)')
ax1.set_yscale('log')
ax1.grid(True, linestyle='--', alpha=0.7)
ax1.legend()

# Highlight outliers in the log plot
if not drop_outliers and not outliers_rlorc_plot.empty:
    ax1.scatter(outliers_rlorc_plot['Query'], outliers_rlorc_plot['ScanTime(s)'],
                color='red', s=50, label=f'RocksDB Lorc Outliers (>{threshold_rlorc:.2f}s)')
if not drop_outliers and not outliers_rbc_plot.empty:
    ax1.scatter(outliers_rbc_plot['Query'], outliers_rbc_plot['ScanTime(s)'],
                color='orange', s=50, label=f'RocksDB BlockCache Outliers (>{threshold_rbc:.2f}s)')
if not drop_outliers and not outliers_blorc_plot.empty:
    ax1.scatter(outliers_blorc_plot['Query'], outliers_blorc_plot['ScanTime(s)'],
                color='darkred', s=50, label=f'BlobDB Lorc Outliers (>{threshold_blorc:.2f}s)')
if not drop_outliers and not outliers_bbc_plot.empty:
    ax1.scatter(outliers_bbc_plot['Query'], outliers_bbc_plot['ScanTime(s)'],
                color='deeppink', s=50, label=f'BlobDB BlobCache Outliers (>{threshold_bbc:.2f}s)')
ax1.legend()

# Plot 2: Regular plot with outliers marked
ax2.plot(df_rbc['Query'], df_rbc['ScanTime(s)'], linestyle='-',
         color='darkgreen', linewidth=0.8, label='RocksDB BlockCache')
ax2.plot(df_rlorc['Query'], df_rlorc['ScanTime(s)'], linestyle='-',
         color='lightgreen', linewidth=0.8, label='RocksDB Lorc')
ax2.plot(df_bbc['Query'], df_bbc['ScanTime(s)'], linestyle='-',
         color='darkblue', linewidth=0.8, label='BlobDB BlobCache')
ax2.plot(df_blorc['Query'], df_blorc['ScanTime(s)'], linestyle='-',
         color='lightblue', linewidth=0.8, label='BlobDB Lorc')
ax2.set_ylabel('Scan Time (seconds)')
ax2.set_xlabel('Operation Number')
ax2.set_title('Scan Performance Comparison (Regular Scale)')
ax2.grid(True, linestyle='--', alpha=0.7)
ax2.legend()

# Mark outliers in the regular plot
if not drop_outliers and not outliers_rlorc_plot.empty:
    ax2.scatter(outliers_rlorc_plot['Query'], outliers_rlorc_plot['ScanTime(s)'],
                color='red', s=50, label=f'RocksDB Lorc Outliers (>{threshold_rlorc:.2f}s)')
if not drop_outliers and not outliers_rbc_plot.empty:
    ax2.scatter(outliers_rbc_plot['Query'], outliers_rbc_plot['ScanTime(s)'],
                color='orange', s=50, label=f'RocksDB BlockCache Outliers (>{threshold_rbc:.2f}s)')
if not drop_outliers and not outliers_blorc_plot.empty:
    ax2.scatter(outliers_blorc_plot['Query'], outliers_blorc_plot['ScanTime(s)'],
                color='darkred', s=50, label=f'BlobDB Lorc Outliers (>{threshold_blorc:.2f}s)')
if not drop_outliers and not outliers_bbc_plot.empty:
    ax2.scatter(outliers_bbc_plot['Query'], outliers_bbc_plot['ScanTime(s)'],
                color='deeppink', s=50, label=f'BlobDB BlobCache Outliers (>{threshold_bbc:.2f}s)')
ax2.legend()

# Add statistical information as text
stats_text_rlorc = (f"RocksDB Lorc Stats ({'Outliers Dropped' if drop_outliers else 'Original Data'}):\n"
                    f"Mean: {mean_rlorc:.4g}s\nStd Dev: {std_rlorc:.4g}s\n"
                    f"Min: {df_rlorc['ScanTime(s)'].min():.4g}s\nMax: {df_rlorc['ScanTime(s)'].max():.4g}s\n"
                    f"Original Outliers: {num_outliers_rlorc}")

stats_text_rbc = (f"RocksDB BlockCache Stats ({'Outliers Dropped' if drop_outliers else 'Original Data'}):\n"
                  f"Mean: {mean_rbc:.4g}s\nStd Dev: {std_rbc:.4g}s\n"
                  f"Min: {df_rbc['ScanTime(s)'].min():.4g}s\nMax: {df_rbc['ScanTime(s)'].max():.4g}s\n"
                  f"Original Outliers: {num_outliers_rbc}")

stats_text_blorc = (f"BlobDB Lorc Stats ({'Outliers Dropped' if drop_outliers else 'Original Data'}):\n"
                    f"Mean: {mean_blorc:.4g}s\nStd Dev: {std_blorc:.4g}s\n"
                    f"Min: {df_blorc['ScanTime(s)'].min():.4g}s\nMax: {df_blorc['ScanTime(s)'].max():.4g}s\n"
                    f"Original Outliers: {num_outliers_blorc}")

stats_text_bbc = (f"BlobDB BlobCache Stats ({'Outliers Dropped' if drop_outliers else 'Original Data'}):\n"
                  f"Mean: {mean_bbc:.4g}s\nStd Dev: {std_bbc:.4g}s\n"
                  f"Min: {df_bbc['ScanTime(s)'].min():.4g}s\nMax: {df_bbc['ScanTime(s)'].max():.4g}s\n"
                  f"Original Outliers: {num_outliers_bbc}")

# Position the text boxes
text_props = dict(bbox=dict(facecolor='wheat', alpha=0.5), verticalalignment='top', fontsize=8)
ax1.text(0.01, 0.98, stats_text_rlorc, transform=ax1.transAxes, **text_props)
ax1.text(0.01, 0.78, stats_text_rbc, transform=ax1.transAxes, **text_props)
ax1.text(0.51, 0.98, stats_text_blorc, transform=ax1.transAxes, **text_props) # Adjusted x for right side
ax1.text(0.51, 0.78, stats_text_bbc, transform=ax1.transAxes, **text_props) # Adjusted x for right side


# Calculate and display performance improvement
comparison_texts = []
if mean_rlorc > 0 and mean_rbc > 0:
    improvement_rdb = ((mean_rbc - mean_rlorc) / mean_rlorc) * 100
    comparison_texts.append(f"RocksDB: Lorc vs BlockCache: {improvement_rdb:.2f}% {'faster with Lorc' if improvement_rdb > 0 else 'faster with BlockCache'}")

if mean_blorc > 0 and mean_bbc > 0:
    improvement_bdb = ((mean_bbc - mean_blorc) / mean_blorc) * 100
    comparison_texts.append(f"BlobDB: Lorc vs BlobCache: {improvement_bdb:.2f}% {'faster with Lorc' if improvement_bdb > 0 else 'faster with BlockCache'}")

if comparison_texts:
    ax1.text(0.5, 0.02, "\n".join(comparison_texts), transform=ax1.transAxes,
             bbox=dict(facecolor='white', alpha=0.7), horizontalalignment='center', verticalalignment='bottom', fontsize=9)

# Optimize display
plt.tight_layout()

# Save chart
plt.savefig(output_image_path, dpi=300)

# Display chart
plt.show()
