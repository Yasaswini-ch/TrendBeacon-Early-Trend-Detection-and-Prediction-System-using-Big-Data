"""
keyword_graph.py — Force-directed graph for related keywords.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import numpy as np

def plot_related_keywords_graph(keyword: str, df: pd.DataFrame) -> go.Figure:
    """
    Render a simulated force-directed cluster of related keywords.
    In a real app, this would use co-occurrence data or word embeddings.
    """
    # Simulate related keywords by picking others from the same trend label
    keyword_row = df[df["keyword"] == keyword]
    if keyword_row.empty:
        # If keyword not found, pick a few random ones
        related = df.sample(min(len(df), 6))
    else:
        label = keyword_row.iloc[0]["trend_label"]
        related = df[df["trend_label"] == label].head(7)
        # Ensure the main keyword is included if not already
        if keyword not in related["keyword"].values:
            related = pd.concat([keyword_row, related.head(6)])

    # Generate random positions for a "force-directed" look
    n = len(related)
    theta = np.linspace(0, 2*np.pi, n, endpoint=False)
    x = np.cos(theta)
    y = np.sin(theta)
    
    # Center the main keyword
    x[0], y[0] = 0, 0
    
    edge_x = []
    edge_y = []
    for i in range(1, n):
        edge_x.extend([0, x[i], None])
        edge_y.extend([0, y[i], None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1, color='#888'),
        hoverinfo='none',
        mode='lines')

    node_trace = go.Scatter(
        x=x, y=y,
        mode='markers+text',
        text=related["keyword"],
        textposition="top center",
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=20,
            color=related["frequency"] if "frequency" in related.columns else 10,
            line_width=2))

    fig = go.Figure(data=[edge_trace, node_trace],
                 layout=go.Layout(
                    title=f'Related Keywords for "{keyword}"',
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=20,l=5,r=5,t=40),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color="white")
                ))
    return fig
