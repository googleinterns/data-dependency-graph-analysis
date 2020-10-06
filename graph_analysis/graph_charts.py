"""
This module implements methods for creating charts using altair.
It allows to implement 2 charts:
    - nodes in cycles graph
    - graph structure distributions
"""

import altair as alt
alt.data_transformers.disable_max_rows()


class GraphCharts:
    """
    A class to draw charts for graph structure understanding.

    ...

    Methods:
        get_cycle_chart()
            Returns bar chart counting unique nodes in cycles.

        get_density_chart(df, density_field, title, scale_x="sqrt", scale_y="sqrt")
            Returns a density chart for number of elements in a group.

        get_graph_structure_charts()
            Returns a combination of density charts.
    """
    def __init__(self, graph_features):
        self.df_cycle_count = graph_features.df_cycle_count
        self.df_without_outliers = graph_features.df_without_outliers

    def get_cycle_chart(self):
        """Returns bar chart counting unique nodes in cycles."""
        cycle_chart = alt.Chart(self.df_cycle_count).mark_bar(
            height=20,
            color="#680101",
            opacity=0.8
        ).encode(
            x = alt.X("count:Q"),
            y = alt.Y("node_type:N", sort='-x')
            ).properties(
            height=200,
            width = 400,
            background = "#f5f3f2",
            padding=30,
            title=alt.TitleParams(text="Distribution of nodes in cycles.",
                                  subtitle=["If this graph is not empty, then datasets and systems need to be checked."],
                                  font="Helvetica Neue",
                                  fontSize=22,
                                  color="#3e454f",
                                  subtitleFont="Helvetica Neue",
                                  subtitleFontSize=16,
                                  subtitleColor="#3e454f",
                                  anchor="start",
                                  dx=20,
                                  dy=-20
                                  )
            ).configure_axis(
                ticks=False,
                labelFont="Helvetica Neue",
                labelFontSize=14,
                labelColor="#494F59",
                labelPadding=10,
                title=None
            )
        return cycle_chart

    @staticmethod
    def get_density_chart(df, density_field, title, scale_x="sqrt", scale_y="sqrt"):
        """
        Returns a density chart for number of elements in a group.
        Axis is scaled, as most values (99%) are from 0-5.
        """
        chart = alt.Chart(df).transform_fold(
            [density_field], as_ = ['Measurement_type', 'value']).transform_density(
                  density='value',
                  bandwidth=0.6,
                  extent= [min(df[density_field]), max(df[density_field])],
                  counts = True,
                  steps=df.shape[0]
              ).mark_area().encode(
                  alt.X('value:Q', scale=alt.Scale(type=scale_x)),
                  alt.Y('density:Q', scale=alt.Scale(type=scale_y), stack='zero')
              ).properties(height=200, width = 200, title=alt.TitleParams(text=title, font="Helvetica Neue",
                              fontSize=12,
                              color="#3e454f"))
        return chart

    def get_graph_structure_charts(self):
        """Returns a combination of density charts."""
        datasets_df, systems_df, dataset_collections_df, system_collections_df = self.df_without_outliers
        datasets_chart = self.get_density_chart(datasets_df,
                                                "datasets_in_collection",
                                                "Datasets in collections distribution.")
        systems_chart = self.get_density_chart(systems_df,
                                               "systems_in_collection",
                                               "Systems in collections distribution.")
        dataset_collections_chart = self.get_density_chart(dataset_collections_df,
                                                           "dataset_collections_in_collection",
                                                           "Dataset collections in collections distribution.")
        system_collections_chart = self.get_density_chart(system_collections_df,
                                                          "system_collections_in_collection",
                                                          "System collections in collections distribution.")

        layered_chart = alt.hconcat(datasets_chart, systems_chart, dataset_collections_chart, system_collections_chart
                                    ).properties(
                            background = "#f5f3f2",
                            padding=30,
                            title=alt.TitleParams(text="Nodes density distributions in a graph.",
                                                  font="Helvetica Neue",
                                                  fontSize=22,
                                                  color="#3e454f",
                                                  subtitleFont="Helvetica Neue",
                                                  subtitleFontSize=16,
                                                  subtitleColor="#3e454f",
                                                  anchor="start",
                                                  dx=20,
                                                  dy=-20
                                                  )
                            ).configure_axis(
                                ticks=False,
                                labelFont="Helvetica Neue",
                                labelFontSize=14,
                                labelColor="#494F59",
                                labelPadding=10,
                                title=None
                            )

        return layered_chart
