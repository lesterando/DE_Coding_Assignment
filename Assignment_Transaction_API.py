from flask import Flask, jsonify
from flask_restful import Api, Resource
import csv, json, glob, os
import pandas as pd


app = Flask(__name__)
api = Api(app)

trxn = pd.DataFrame()
product = pd.DataFrame()
latest_trxn_file = ''

TRANSACTION_PATH = r'C:\TRANSACTION\*.csv'
PRODUCT_REFERENCE_PATH = r'C:\PRODUCT_REFERENCE\ProductReference.csv'


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


def read_latest_transaction_csv():
    list_of_files = glob.glob(TRANSACTION_PATH)
    latest_trxn_file = str(max(list_of_files, key=os.path.getmtime))
    
    return latest_trxn_file


def merge_df(trxn, product):
    trxn = pd.read_csv(read_latest_transaction_csv(), skipinitialspace=True, encoding='utf-8_sig')
    product = pd.read_csv(PRODUCT_REFERENCE_PATH, skipinitialspace=True, encoding='utf-8_sig')

    # trim whitespaces of columns and merge files
    trxn.columns = trxn.columns.str.strip()
    product.columns = product.columns.str.strip()       
    trxn_product = pd.merge(left=trxn, right=product, on='productId')
    
    return trxn_product

    
class AssignmentTransaction(Resource):
    def get(self, trxn_id):    
        trxn_product = merge_df(trxn, product)
        
        try:
            # find the transaction and get the selected fields
            trxn_fetched = trxn_product.loc[trxn_product["transactionId"] == trxn_id]
            trxn_fieldselect = trxn_fetched[['transactionId', 'productName', 'transactionAmount', 'transactionDatetime']]
            
            # convert the dataframe to JSON
            obj_rec = trxn_fieldselect.to_dict(orient="records")[0]
     
            return jsonify(obj_rec)
        except:
            return jsonify({ "error:" : "not found"})

        
class AssignmentTransactionSummaryByProducts(Resource):
    def get(self, last_n_days):        
        trxn_product = merge_df(trxn, product)
        
        # set transactionDatetime as index
        trxn_product['transactionDatetime'] = pd.to_datetime(trxn_product['transactionDatetime'])
        trxn_product.set_index('transactionDatetime', inplace=True)
        
        # get the current transaction up to last_n_days
        last_n_days_trxn = trxn_product.sort_index().last(str(last_n_days) + 'D')
        df_last_n_days_trxn = last_n_days_trxn.groupby(['productName'])['transactionAmount'].sum()
        summary_trxn = df_last_n_days_trxn.to_dict()
        
        return jsonify({"summary": [summary_trxn]}) 
        
            
class AssignmentTransactionSummaryByManufacturingCity(Resource):
    def get(self, last_n_days):        
        trxn_product = merge_df(trxn, product)

        # set transactionDatetime as index
        trxn_product['transactionDatetime'] = pd.to_datetime(trxn_product['transactionDatetime'])
        trxn_product.set_index('transactionDatetime', inplace=True)
        
        # get the current transaction up to last_n_days
        last_n_days_trxn = trxn_product.sort_index().last(str(last_n_days) + 'D')
        df_last_n_days_trxn = last_n_days_trxn.groupby(['productManufacturingCity'])['transactionAmount'].sum()
        summary_trxn = df_last_n_days_trxn.to_dict()
        
        return jsonify({"summary": [summary_trxn]})          


api.add_resource(AssignmentTransaction, "/assignment/transaction/<int:trxn_id>")
api.add_resource(AssignmentTransactionSummaryByProducts, "/assignment/transactionSummaryByProducts/<int:last_n_days>")
api.add_resource(AssignmentTransactionSummaryByManufacturingCity, "/assignment/transactionSummaryByManufacturingCity/<int:last_n_days>")


if __name__ == '__main__':
    app.run(debug=True, port=8080)