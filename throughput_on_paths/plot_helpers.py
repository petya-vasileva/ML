import pandas as pd
import numpy as np
import plotly.graph_objects as go
import matplotlib.pyplot as plt  # Required for colormap

def generate_plotly_heatmap_with_hover(df, src_site, dest_site):
    # Filter the DataFrame for the specified source and destination site
    filtered_df = df[(df['src_site'] == src_site) & (df['dest_site'] == dest_site)][['ttls-hops_hash', 'rid', 'ttl', 'router', 'destination_reached']].drop_duplicates()

    if len(filtered_df) > 0:
        # Pivot the DataFrame
        pivot_df = filtered_df.pivot(index='ttls-hops_hash', columns='ttl', values='router')

        # Get unique RIDs including 'unknown'
        unique_rids = pd.Series(pivot_df.stack().unique()).dropna().tolist()

        # Create a color map for each router, 'unknown' is black, and empty is white
        color_list = ['#FFFFFF'] + [f"rgba({int(c[0]*255)}, {int(c[1]*255)}, {int(c[2]*255)}, 1)" for c in plt.cm.tab20(np.linspace(0, 1, len(unique_rids)))]
        if 'unknown' in unique_rids:
            color_list[unique_rids.index('unknown') + 1] = 'black'  # Offset by 1 due to white color for NaN

        # Replace RIDs with their corresponding index in the unique_rids list
        rid_to_index = {rid: i for i, rid in enumerate(unique_rids)}
        index_df = pivot_df.applymap(lambda x: rid_to_index[x] if pd.notna(x) else -1)

        # Extract the destination_reached column and align with the pivot table
        destination_reached = filtered_df[['ttls-hops_hash', 'destination_reached']].drop_duplicates()
        destination_reached.set_index('ttls-hops_hash', inplace=True)

        # Prepare hover text with the rid, ttls-hops_hash, and router
        hover_text = np.empty(pivot_df.shape, dtype=object)
        for i, (ttls_hops, row) in enumerate(pivot_df.iterrows()):
            for j, router in enumerate(row):
                if pd.notna(router):
                    rid = filtered_df[(filtered_df['ttls-hops_hash'] == ttls_hops) & (filtered_df['ttl'] == row.index[j])]['rid'].values[0]
                    hover_text[i, j] = f"RID: {rid}<br>ttls-hops_hash: {ttls_hops}<br>Router: {router}"
                else:
                    hover_text[i, j] = "Missing"

        # Create the Plotly heatmap
        fig = go.Figure()

        heatmap = go.Heatmap(
            z=index_df.values,
            x=index_df.columns,
            y=index_df.index,
            colorscale=color_list,
            zmin=-1,
            zmax=len(unique_rids) - 1,
            text=hover_text,
            hoverinfo='text',
            showscale=False,
            xgap=1,  # Add gap between cells for horizontal lines
            ygap=1,  # Add gap between cells for vertical lines
        )

        fig.add_trace(heatmap)

        # Add title and axis labels
        fig.update_layout(
            title=f"Path signature between {src_site} and {dest_site}",
            xaxis_title='TTL',
            yaxis_title='Path Identifier (ttls-hops_hash)',
        )

        # Adding second Y-axis as annotations for 'destination_reached'
        annotations = []
        for i, (path_id, reached) in enumerate(destination_reached.itertuples()):
            annotations.append(dict(
                xref='paper',
                yref='y',
                x=1.05,  # Position outside the heatmap
                y=i,
                xanchor='left',
                text=str(reached),
                showarrow=False
            ))

        annotations.append(dict(
            xref='x domain',
            yref='paper',
            x=1.02,  # Align with destination reached annotations
            y=1.1,  # Slightly above the annotations to serve as the label
            xanchor='left',
            text='Destination Reached',  # Label for the second Y-axis
            showarrow=False,
            font=dict(size=12, color='black')
        ))

        fig.update_layout(
            annotations=annotations,
            margin=dict(r=150),  # Adjust margin to fit annotations
            height=600
        )

        fig.show()
    else:
        print(f'There are no paths between {src_site} - {dest_site}')

