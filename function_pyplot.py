# pip install matplotlib plotly kaleido
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import numpy as np
from datetime import timedelta, timezone
import os

PLOT_ENABLE = False
PLOTLY_ENABLE = True

colors= ['red', 'green', 'blue', 'orange', 'purple', 
         'yellow', 'pink', 'brown', 'cyan', 'magenta', 
         'lime', 'navy', 'teal', 'coral', 'gold', 
         'salmon', 'plum', 'khaki', 'lavender']


def cluster_graphical_metrics_plot(metric_name, cluster, instances, 
                          metric_data, plots, extended_metric_data, extended_plots,
                          is_show, is_save):
    if not PLOT_ENABLE:
        print("PLOT_ENABLE is False, skipping matplotlib plot.")
        return
    
    os.makedirs(f"metrics/{cluster}", exist_ok=True)

    # some of instance may not have metrics data
    graphic_len = len(metric_data)
    # print(f"graphic_len: {graphic_len}")
    fig, axes = plt.subplots(graphic_len, 1, figsize=(12, 6), sharex=False, sharey=False)
    plt.title(f"{metric_name} for cluster {cluster}", fontsize=20, pad=20)

    axes = np.atleast_1d(axes)
    filename = f"metrics/{cluster}/{cluster}-{metric_name}-plot.png"

    graphic_idx = 0
    for instance in instances:
        if metric_data.get(instance) is None or extended_metric_data.get(instance) is None:
            continue
        title = f"{metric_name} for {instance if instance != 'NO-INSTANCE' else cluster}"
        # Extract timestamps and values from the list of datapoints
        timestamps = [dp['Timestamp'] for dp in metric_data[instance]]
        ylabel = metric_data[instance][0]["Unit"]

        # plt.figure(figsize=(12, 6))

        idx = 0
        for plot in plots:
            values = [dp[plot] for dp in metric_data[instance] if plot in dp]
            axes[graphic_idx].plot(timestamps, values, color=colors[idx], linewidth=2, label=plot)
            idx += 1
        for plot in extended_plots:
            values = [dp["ExtendedStatistics"][plot] for dp in extended_metric_data[instance] if plot in dp["ExtendedStatistics"]]
            axes[graphic_idx].plot(timestamps, values, color=colors[idx], linewidth=2, label=plot)
            idx += 1
    
        axes[graphic_idx].set_title(title, fontsize=14, pad=20)
        axes[graphic_idx].set_xlabel('Time', fontsize=12)
        axes[graphic_idx].set_ylabel(ylabel, fontsize=12)
        axes[graphic_idx].grid(True, linestyle='--', alpha=0.7)

        axes[graphic_idx].legend()

        graphic_idx += 1
    
    tokyo_timezone = timezone(timedelta(hours=9))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M', tz=tokyo_timezone))
    plt.gcf().autofmt_xdate()
    plt.xticks(rotation=45)
    
    plt.tight_layout()

    if is_save:
        plt.savefig(filename, dpi=300)
    if is_show:
        plt.show()


def cluster_graphical_metrics_plotly(metric_name, cluster, instances, 
                          metric_data, plots, extended_metric_data, extended_plots,
                          is_show, is_save):
    if not PLOTLY_ENABLE:
        print("PLOTLY_ENABLE is False, skipping plotly plot.")
        return
    
    os.makedirs(f"metrics/{cluster}", exist_ok=True)
    # some of instance may not have metrics data
    graphic_len = len(metric_data)
    # print(f"graphic_len: {graphic_len}")

    title = f"{metric_name} for cluster {cluster}"
    filename = f"metrics/{cluster}/{cluster}-{metric_name}-plotly"
    

    # https://plotly.com/python-api-reference/generated/plotly.subplots.make_subplots.html
    # fig = make_subplots(specs=[[{"secondary_y": False}]])
    fig = make_subplots(rows=graphic_len, cols=1, 
                   shared_xaxes=False,
                   shared_yaxes=False,
                   subplot_titles=[f"{instance if instance != 'NO-INSTANCE' else cluster}" for instance in instances])
    
    graphic_idx = 0
    for instance in instances:
        if metric_data.get(instance) is None or extended_metric_data.get(instance) is None:
            continue
        
        # Extract timestamps and values from the list of datapoints
        timestamps = [dp['Timestamp'] for dp in metric_data[instance]]
        ylabel = metric_data[instance][0]["Unit"]

        idx = 0
        for plot in plots:
            values = [dp[plot] for dp in metric_data[instance] if plot in dp]
            fig.add_trace(
                go.Scatter(
                    x=timestamps, 
                    y=values,
                    mode='lines',
                    name=plot,
                    legendgroup=plot,
                    showlegend=False if graphic_idx > 0 else True, 
                    line=dict(color=colors[idx], width=3)
                ),
                row=graphic_idx+1, col=1
            )
            idx += 1
        for plot in extended_plots:
            values = [dp["ExtendedStatistics"][plot] for dp in extended_metric_data[instance] if plot in dp["ExtendedStatistics"]]
            fig.add_trace(
                go.Scatter(
                    x=timestamps, 
                    y=values,
                    mode='lines',
                    name=plot,
                    legendgroup=plot,
                    showlegend=False if graphic_idx > 0 else True,
                    line=dict(color=colors[idx], width=3)
                ),
                row=graphic_idx+1, col=1
            )
            idx += 1
        
        fig.update_xaxes(title_text="Time", row=graphic_idx+1, col=1)
        fig.update_yaxes(title_text=ylabel, row=graphic_idx+1, col=1)

        graphic_idx += 1
    
    fig.update_layout(
            title=title,
            title_font_size=20,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            template='plotly_white',  # similar to AWS Console
            hovermode='x unified',    # 
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
    fig.update_yaxes(rangemode='tozero')
    fig.update_xaxes(
            showgrid=True, 
            gridwidth=1, 
            gridcolor='rgba(0,0,0,0.1)'
        )
    fig.update_yaxes(
            showgrid=True, 
            gridwidth=1, 
            gridcolor='rgba(0,0,0,0.1)'
        )
    
    if is_save:
        fig.write_html(f"{filename}.html")

        # on windows, must install this patch or write_image will freeze
        # https://community.plotly.com/t/static-image-export-hangs-using-kaleido/61519/3
        # https://github.com/plotly/Kaleido/releases/tag/v0.1.0.post1
        # pip install kaleido-0.1.0.post1-py2.py3-none-win_amd64.whl
        fig.write_image(f"{filename}.png", width=1200, height=600, scale=2)

    if is_show:
        fig.show()
