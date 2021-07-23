import statsmodels.api as sm
from sklearn.metrics import r2_score
from statsmodels.tools import eval_measures

def regression(train_df, test_df, ind_var_names, dep_var_name):
    X_train = train_df[ind_var_names].to_numpy()
    X_test = test_df[ind_var_names].to_numpy()
    y_train = train_df[dep_var_name].to_numpy()
    y_test = test_df[dep_var_name].to_numpy()
    
    X_train = sm.add_constant(X_train)
    X_test = sm.add_constant(X_test)

    mod = sm.OLS(y_train, X_train)
    res = mod.fit()

    train_pred_vals = res.predict(X_train)
    mse_train = eval_measures.mse(y_train, train_pred_vals)

    test_pred_vals = res.predict(X_test)
    mse_test = eval_measures.mse(y_test, test_pred_vals)

    rsquared_val = r2_score(y_test, test_pred_vals)

    print(res.summary())
    
    return mse_train, mse_test, rsquared_val