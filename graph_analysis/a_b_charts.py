import altair as alt


class ABCharts:
    def __init__(self, ab_features):
        self.paths_df = ab_features.paths_df
        self.point_df = ab_features.point_df
        self.colors = ["#36f1cd", "#fe5f55", "#f7b32b", "#118ab2", "#073b4c"]
        self.slo_df = ab_features.slo_df
        self.data_integrity_df = ab_features.data_integrity_df

    @staticmethod
    def draw_path(df, color):
        path_selection = alt.selection_single()
        chart = alt.Chart(df).mark_line(color=color, strokeWidth=5).encode(
            alt.X('x', scale=alt.Scale(zero=False), sort=None),
            alt.Y('y', scale=alt.Scale(zero=False), sort=None),
            opacity=alt.condition(path_selection, alt.value(1), alt.value(0.4)),
            color=alt.condition(path_selection, alt.value(color), alt.value("#bfc0c0"))
            ).add_selection(path_selection)
        return chart

    def draw_nodes(self, point_df, max_paths=3):
        point_selection = alt.selection_single(fields=["path"])
        scatter_plot = alt.Chart(point_df[point_df.path < max_paths].reset_index()).mark_point(
            size=400,
            opacity=1,
            filled=True
        ).encode(
            x=alt.X('x:Q', sort=None, axis=None),
            y=alt.Y('y:Q', sort=None, axis=None),
            color=alt.Color("path", scale=alt.Scale(domain=list(range(max_paths)), range=self.colors), legend=None),
            opacity=alt.condition(point_selection, alt.value(1), alt.value(0.4)),
            shape=alt.Shape('type:N', scale=alt.Scale(range=['circle', 'diamond']), legend=None),
            tooltip="index"
            ).add_selection(point_selection)
        return scatter_plot

    def get_graph_chart(self, max_paths=3):
        line_charts = [self.draw_path(self.paths_df[i], self.colors[i]) for i in range(min(max_paths,
                                                                                  len(self.paths_df)))][::-1]
        scatter_plot = self.draw_nodes(self.point_df)
        base_chart = line_charts[0]
        for chart in line_charts[1:]:
            base_chart += chart
        base_chart += scatter_plot
        graph_chart = base_chart.properties(
            width=900,
            background = "#f5f3f2",
            padding=30,
            title=alt.TitleParams(text="All the paths between dataset A and dataset B.",
                                  subtitle=["Shortest path is displayed in the middle."],
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
            ).configure_view(strokeOpacity=0)
        return graph_chart

    def get_slo_chart(self):
        total_slo = list(slo_df.slo)[-1]
        if total_slo < 6 * 60:
            label_interval, label_variable, time_div = 10, "s", 1
        elif total_slo < 100 * 60:
            label_interval, label_variable, time_div = 60, "m", 60
        elif total_slo < 30 * 60 * 60:
            label_interval, label_variable, time_div = 60 * 60, "h", 60 * 60
        else:
            label_interval, label_variable, time_div =  24 * 60 * 60, "d",  24 * 60 * 60
        label_expression = f"datum.value % {label_interval} ? null : datum.value / {time_div} + '{label_variable}'"

        chart = alt.Chart(self.slo_df).mark_line(color='#db646f').encode(
            x=alt.X('slo:Q', sort=None, axis=alt.Axis(labelExpr=label_expression)),
            y=alt.Y('dataset_id:N', sort=None),
            detail='dataset_id:N'
        )

        chart += alt.Chart(self.slo_df).mark_point(
            size=400,
            opacity=1,
            filled=True
        ).encode(
            x=alt.X('slo:Q', sort=None, axis=alt.Axis(labelExpr=label_expression)),
            y=alt.Y('dataset_id:N', sort=None),
            color=alt.Color('dataset_id:O', legend=None,
                scale=alt.Scale(
                    range=['#e6959c', '#911a24']
                ),
            )
        )

        slo_chart = chart.properties(
            width=700,
            background = "#f5f3f2",
            padding=30,
            title=alt.TitleParams(text="SLO between A and B nodes.",
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
        return slo_chart

    @staticmethod
    def get_integrity_charts(base, time_value, time_string, x_name):
        chart = base.mark_bar().encode(
                    y=alt.Y('dataset_id:N', sort=None),
                    x=alt.X(time_value, axis=None),
                ).properties(title=alt.TitleParams(
                    text=f"Dataset {x_name} time.",
                    font="Helvetica Neue",
                    fontSize=15,
                    color="#3e454f",
                    anchor="start"))
        labels = chart.mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,
                    font="Helvetica Neue",
                    fontSize=12,
                    color="#3e454f").encode(
                    text=alt.Text(f'{time_string}:N')
                )
        return chart, labels

    def get_data_integrity_chart(self):
        base = alt.Chart(self.data_integrity_df)

        rec, rec_labels = self.get_integrity_charts(base, "reconstruction_time_duration", "reconstruction_time_string","reconstruction")
        reg, reg_labels = self.get_integrity_charts(base, "regeneration_time_duration", "regeneration_time_string","regeneration")
        rest, rest_labels = self.get_integrity_charts(base, "restoration_time_duration", "restoration_time_string", "backup restoration")

        data_integrity_chart = alt.vconcat((rec + rec_labels), (reg + reg_labels), (rest + rest_labels)
                                ).configure_view(strokeOpacity=0).properties(
                                background="#E5E2E0",
                                title=alt.TitleParams(text="Data integrity in the stream parameters.", font="Helvetica Neue",
                                                      fontSize=22,
                                                      color="#3e454f",
                                                      subtitleFont="Helvetica Neue",
                                                      subtitleFontSize=16,
                                                      subtitleColor="#3e454f",
                                                      anchor="start",
                                                      offset=20
                                                      ),
                                padding=20
                                ).configure_axis(
                                    ticks=False,
                                    labelFont="Helvetica Neue",
                                    labelFontSize=14,
                                    labelColor="#494F59",
                                    titleFont="Helvetica Neue",
                                    titleFontSize=14,
                                    titleColor="#494F59",
                                    titlePadding=10,
                                    labelPadding=10,
                                    title=None
                                )

        return data_integrity_chart
