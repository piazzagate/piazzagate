import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import sys
from pathlib import Path
sys.path.insert(1, 'code/analysis')
from util import get_demographic_data


DATA_DIR = Path(__file__).parent.parent.parent.parent.parent / 'data'

def tweet_rate_demographic_scatterplots(df):
    features = ['total_population',
                'per_capita_income',
                'percent_high_school_graduate', 
                'percent_households_with_Internet',
                'percent_votes_democrat',
                'percent_votes_republican']

    plt_idxs = [(0,0),(0,1),(0,2),(1,0),(1,1),(1,2)]

    fig, axs = plt.subplots(2, 3, figsize=(10, 8), sharey=True)
    fig.suptitle('# misinfo tweets per million people vs. county stats', fontsize=24)
    for feature, (i,j) in zip(features, plt_idxs):
        ax = axs[i][j]
        sns.scatterplot(x=df[feature], y=df['tweet_rate'], alpha=0.2, ax=ax)
        m, b = np.polyfit(df[feature], df['tweet_rate'], 1)
        ax.plot(df[feature], m*df[feature] + b, label=f'{np.round(m, 3)}x + {int(b)}')
        ax.set(xlabel=feature.replace('_', ' '), ylabel='# misinformation tweets per million people')
        ax.legend(loc='upper right')

    fig.savefig(DATA_DIR / 'analysis' / 'demographics' / 'official.png')

def main():
    train_df = get_demographic_data(DATA_DIR, 'processed_random_train')
    tweet_rate_demographic_scatterplots(train_df)

if __name__ == "__main__":
    main()
