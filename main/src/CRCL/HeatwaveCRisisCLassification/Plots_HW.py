# Created Date: 08/06/2018
# Modified Date:
#
#   Main function to create TOPICS 104 for the Heatwave pilot
#

import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Wedge, Rectangle
import numpy as np

# multiple line plot
def multiple_line_plot(directory, Frame, value):
    Frame_piv = Frame.pivot_table(index='DateTime', columns='Name', values=value)

    # Create lineplot figure
    plt.figure(figsize=(20, 10))

    # style
    plt.style.use('seaborn-darkgrid')

    # create a color palette
    palette = plt.get_cmap('Set1')

    # multiple line plot
    num = 0
    for column in Frame_piv:
        num += 1

        plt.plot(Frame_piv.index, Frame_piv[column], marker='', color=palette(num), linewidth=1, alpha=0.9,
                 label=column)

        # Add legend
        plt.legend(loc=2, ncol=2)

        # Add titles
        title_value = value + " plot per Interest point"
        plt.title(title_value, loc='center', fontsize=14, fontweight=0, color='red')
        plt.legend(loc='best', fontsize=12)
        # plt.legend(loc='upper right', bbox_to_anchor=(1.25, 1.0))
        plt.xlabel("Datetime", fontsize=12)
        ylabel_value = value + ' Score'
        plt.ylabel(ylabel_value, fontsize=12)
        # plt.xticks(rotation=45, fontsize=12)

        xlab_Val = []
        for i in range(len(Frame_piv.index)):
            string = Frame_piv.index[i].split("T")[0].split("-")[2] + "-" + Frame_piv.index[i].split("T")[0].split("-")[
                1]
            string1 = Frame_piv.index[i].split("T")[1].split(':')[0] + ':' + \
                      Frame_piv.index[i].split("T")[1].split(':')[2]
            string2 = string + " " + string1
            xlab_Val.append(string2)
        # print(xlab_Val)

    x = np.arange(len(xlab_Val))
    plt.xticks(x, (xlab_Val), fontsize=10, rotation=35)

    # plt.show()

    # Save figure
    figname = directory + "/" + value + "_per_point.png"
    plt.savefig(figname)
    plt.close()


# ----------------------------------------------------------------------
# Create a bar plot
# def bar_plot_count(directory, dset, group_var):
#     piv_dset = dset.pivot_table(columns=group_var, index='Name', values='Count', aggfunc=np.sum)
#
#     # Create lineplot figure
#     plt.figure(figsize=(20, 10))
#
#     piv_dset.plot(kind='bar')
#
#     plt.title("Discomfort Index per Interest point", loc='center', fontsize=14, fontweight=0, color='red')
#     plt.legend(loc='best', fontsize=10)
#     plt.xlabel("Name", fontsize=12)
#     plt.ylabel("Count", fontsize=12)
#     plt.xticks(rotation=0, fontsize=12)
#
#     # plt.show()
#
#     # Save figure
#     figname = directory + "/" + "barplot_FireDanger_per_point.png"
#     plt.savefig(figname)
#     plt.close()


# ----------------------------------------------------------------------------
# Gauge Plot

def degree_range(n):
    start = np.linspace(0, 180, n + 1, endpoint=True)[0:-1]
    end = np.linspace(0, 180, n + 1, endpoint=True)[1::]
    mid_points = start + ((end - start) / 2.)
    return np.c_[start, end], mid_points


def rot_text(ang):
    rotation = np.degrees(np.radians(ang) * np.pi / np.pi - np.radians(90))
    return rotation


def gauge(labels, colors, arrow=1, title='', fname=False):
    """
    some sanity checks first

    """

    N = len(labels)

    if arrow > N:
        raise Exception("\n\nThe category ({}) is greated than \
        the length\nof the labels ({})".format(arrow, N))

    """
    if colors is a string, we assume it's a matplotlib colormap
    and we discretize in N discrete colors 
    """

    if isinstance(colors, str):
        cmap = cm.get_cmap(colors, N)
        cmap = cmap(np.arange(N))
        colors = cmap[::-1, :].tolist()
    if isinstance(colors, list):
        if len(colors) == N:
            colors = colors[::-1]
        else:
            raise Exception("\n\nnumber of colors {} not equal \
            to number of categories{}\n".format(len(colors), N))

    """
    begins the plotting
    """

    fig, ax = plt.subplots()

    ang_range, mid_points = degree_range(N)

    labels = labels[::-1]

    """
    plots the sectors and the arcs
    """
    patches = []
    for ang, c in zip(ang_range, colors):
        # sectors
        patches.append(Wedge((0., 0.), .4, *ang, facecolor='w', lw=2))
        # arcs
        patches.append(Wedge((0., 0.), .4, *ang, width=0.10, facecolor=c, lw=2, alpha=0.5))

    [ax.add_patch(p) for p in patches]

    """
    set the labels (e.g. 'LOW','MEDIUM',...)
    """

    for mid, lab in zip(mid_points, labels):
        ax.text(0.35 * np.cos(np.radians(mid)), 0.35 * np.sin(np.radians(mid)), lab, \
                horizontalalignment='center', verticalalignment='center', fontsize=8, \
                fontweight='bold', rotation=rot_text(mid))

    """
    set the bottom banner and the title
    """
    r = Rectangle((-0.4, -0.1), 0.8, 0.1, facecolor='w', lw=2)
    ax.add_patch(r)

    ax.text(0, -0.05, title, horizontalalignment='center', \
            verticalalignment='center', fontsize=12, fontweight='bold')

    """
    plots the arrow now
    """
    pos = mid_points[ int(abs(arrow - N)) ]

    ax.arrow(0, 0, 0.225 * np.cos(np.radians(pos)), 0.225 * np.sin(np.radians(pos)), \
             width=0.04, head_width=0.09, head_length=0.1, fc='k', ec='k')

    ax.add_patch(Circle((0, 0), radius=0.02, facecolor='k'))
    ax.add_patch(Circle((0, 0), radius=0.01, facecolor='w', zorder=11))

    """
    removes frame and ticks, and makes axis equal and tight
    """
    ax.set_frame_on(False)
    ax.axes.set_xticks([])
    ax.axes.set_yticks([])
    ax.axis('equal')
    plt.tight_layout()
    if fname:
        fig.savefig(fname, dpi=200)