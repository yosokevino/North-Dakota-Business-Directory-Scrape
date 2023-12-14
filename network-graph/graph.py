import matplotlib.pyplot as plt
import networkx as nx
import random
import pandas as pd

owner_df = pd.read_csv("response-data\owner_data.csv")

G = nx.from_pandas_edgelist(owner_df,source="company_name_upper",target="owner_name_upper",create_using=nx.MultiGraph())
pos = nx.nx_agraph.graphviz_layout(G, prog="circo")
plt.figure(1, figsize=(60, 40))
pos_higher = {}
y_off = 15  # offset on the y axis
for k, v in pos.items():
    pos_higher[k] = (v[0], v[1]+y_off)
nx.draw_networkx_labels(G, pos_higher, font_size=12, font_color='black',font_weight='bold')
C = (G.subgraph(c) for c in nx.connected_components(G))
for g in C:
    c = [random.random()] * nx.number_of_nodes(g)  # random color...
    nx.draw(g, pos, node_size=200, node_color=c, vmin=0.0, vmax=1.0)
plt.show()