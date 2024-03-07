from grafanalib.core import *

dashboard = Dashboard(
    title="Reddit Metrics Dashboard",
    rows=[
        Row(panels=[
            Histogram(
                title="Posts Distribution Over Time",
                targets=[
                    Target(
                        expr='count()',
                        legendFormat='Posts',
                        refId='A',
                    ),
                ],
                bucketAggs=[
                    Histogram(
                        field='created',
                        interval='1d',  # Adjust the interval as needed
                        label='Posts',
                    ),
                ],
            ),
        ]),
        Row(panels=[
            Graph(
                title="Upvote Ratio",
                targets=[
                    Target(
                        expr='avg(upvote_ratio)',
                        legendFormat='Average Upvote Ratio',
                        refId='A',
                    ),
                ],
            ),
        ]),
        Row(panels=[
            Graph(
                title="Score Over Time",
                targets=[
                    Target(
                        expr='avg(score)',
                        legendFormat='Average Score',
                        refId='A',
                    ),
                ],
            ),
        ]),
    ],
).auto_panel_ids()
print (dashboard.to_json())