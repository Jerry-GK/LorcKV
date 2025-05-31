#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Control variable 
with_update = True
enable_dio = True

# Generate file paths based on with_update variable
update_suffix = "_update" if with_update else "_noupdate"
dio_suffix = "_dio" if enable_dio else "_nodio"
lorc_csv_path = f'./csv/scan_performance_lorc{update_suffix}{dio_suffix}.csv'
blobcache_csv_path = f'./csv/scan_performance_blobcache{update_suffix}{dio_suffix}.csv'
output_image_path = f'./scan_performance_comparison{update_suffix}{dio_suffix}.png'

# Read CSV files
df_lorc = pd.read_csv(lorc_csv_path)
df_blobcache = pd.read_csv(blobcache_csv_path)

# Data analysis for Lorc
mean_lorc = df_lorc['ScanTime(s)'].mean()
std_lorc = df_lorc['ScanTime(s)'].std()
threshold_lorc = mean_lorc + 2 * std_lorc
outliers_lorc = df_lorc[df_lorc['ScanTime(s)'] > threshold_lorc]

# Data analysis for BlobCache
mean_blob = df_blobcache['ScanTime(s)'].mean()
std_blob = df_blobcache['ScanTime(s)'].std()
threshold_blob = mean_blob + 2 * std_blob
outliers_blob = df_blobcache[df_blobcache['ScanTime(s)'] > threshold_blob]

# Create figure with subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [2, 1]})

# Plot 1: Log scale plot for both datasets
ax1.plot(df_lorc['Query'], df_lorc['ScanTime(s)'], linestyle='-', 
         color='blue', linewidth=1, label='Lorc')
ax1.plot(df_blobcache['Query'], df_blobcache['ScanTime(s)'], linestyle='-', 
         color='green', linewidth=1, label='BlobCache')
ax1.set_ylabel('Scan Time (seconds) - Log Scale')
ax1.set_xlabel('Query Number')
ax1.set_title('Scan Performance Comparison (Log Scale)')
ax1.set_yscale('log')
ax1.grid(True, linestyle='--', alpha=0.7)
ax1.legend()

# Highlight outliers in the log plot
if not outliers_lorc.empty:
    ax1.scatter(outliers_lorc['Query'], outliers_lorc['ScanTime(s)'], 
                color='red', s=50, label=f'Lorc Outliers (>{threshold_lorc:.2f}s)')
if not outliers_blob.empty:
    ax1.scatter(outliers_blob['Query'], outliers_blob['ScanTime(s)'], 
                color='orange', s=50, label=f'BlobCache Outliers (>{threshold_blob:.2f}s)')
ax1.legend()

# Plot 2: Regular plot with outliers marked
ax2.plot(df_lorc['Query'], df_lorc['ScanTime(s)'], linestyle='-', 
         color='blue', linewidth=1, label='Lorc')
ax2.plot(df_blobcache['Query'], df_blobcache['ScanTime(s)'], linestyle='-', 
         color='green', linewidth=1, label='BlobCache')
ax2.set_ylabel('Scan Time (seconds)')
ax2.set_xlabel('Query Number')
ax2.set_title('Scan Performance Comparison (Regular Scale)')
ax2.grid(True, linestyle='--', alpha=0.7)
ax2.legend()

# Mark outliers in the regular plot
if not outliers_lorc.empty:
    ax2.scatter(outliers_lorc['Query'], outliers_lorc['ScanTime(s)'], 
                color='red', s=50, label=f'Lorc Outliers (>{threshold_lorc:.2f}s)')
if not outliers_blob.empty:
    ax2.scatter(outliers_blob['Query'], outliers_blob['ScanTime(s)'], 
                color='orange', s=50, label=f'BlobCache Outliers (>{threshold_blob:.2f}s)')
ax2.legend()

# Add statistical information as text
stats_text_lorc = (f"Lorc Stats:\n"
                   f"Mean: {mean_lorc:.4f}s\nStd Dev: {std_lorc:.4f}s\n"
                   f"Min: {df_lorc['ScanTime(s)'].min():.4f}s\nMax: {df_lorc['ScanTime(s)'].max():.4f}s\n"
                   f"Outliers: {len(outliers_lorc)} queries")

stats_text_blob = (f"BlobCache Stats:\n"
                   f"Mean: {mean_blob:.4f}s\nStd Dev: {std_blob:.4f}s\n"
                   f"Min: {df_blobcache['ScanTime(s)'].min():.4f}s\nMax: {df_blobcache['ScanTime(s)'].max():.4f}s\n"
                   f"Outliers: {len(outliers_blob)} queries")

# Position the text boxes
ax1.text(0.02, 0.95, stats_text_lorc, transform=ax1.transAxes, 
         bbox=dict(facecolor='lightblue', alpha=0.7), verticalalignment='top')
ax1.text(0.02, 0.65, stats_text_blob, transform=ax1.transAxes, 
         bbox=dict(facecolor='lightgreen', alpha=0.7), verticalalignment='top')

# Calculate and display performance improvement
if mean_lorc > 0:  # Avoid division by zero
    improvement = ((mean_blob - mean_lorc) / mean_lorc) * 100
    comparison_text = f"Performance Difference: {improvement:.2f}% " + ("faster with Lorc" if improvement > 0 else "faster with BlobCache")
    ax1.text(0.5, 0.02, comparison_text, transform=ax1.transAxes,
             bbox=dict(facecolor='white', alpha=0.7), horizontalalignment='center')

# Optimize display
plt.tight_layout()

# Save chart
plt.savefig(output_image_path, dpi=300)

# Display chart
plt.show()
