# FILE: services/usage-api/main.py (FIXED)
# Usage API - REST API for querying usage data

from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from cassandra.cluster import Cluster
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
auth = HTTPBasicAuth()

# Basic auth credentials from environment
API_USER = os.getenv('API_USER', 'api_user')
API_PASSWORD = os.getenv('API_PASSWORD', 'api_password')

USERS = {
    API_USER: API_PASSWORD
}

@auth.verify_password
def verify_password(username, password):
    if username in USERS and USERS[username] == password:
        return username
    return None

class UsageAPI:
    def __init__(self, scylla_hosts):
        self.cluster = Cluster(scylla_hosts)
        self.session = self.cluster.connect('cdr_usage')
        
    def get_usage(self, msisdn, start_time, end_time):
        """Query usage data from ScyllaDB"""
        
        # Parse timestamps
        try:
            start_dt = datetime.strptime(start_time, "%Y%m%d%H%M%S")
            end_dt = datetime.strptime(end_time, "%Y%m%d%H%M%S")
        except ValueError:
            return None, "Invalid timestamp format. Use YYYYMMDDHHmmss"
        
        # Query by date range
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
        
        query = """
            SELECT date, hour, data_type, up_bytes, down_bytes, 
                   call_duration_sec, call_count, updated_at
            FROM daily_usage
            WHERE msisdn = ? AND date >= ? AND date <= ?
        """
        
        rows = self.session.execute(query, (msisdn, start_date, end_date))
        
        # Format response
        usage_data = []
        for row in rows:
            # Determine category
            if row.up_bytes > 0 or row.down_bytes > 0:
                category = "data"
                usage_type = row.data_type
                total = row.up_bytes + row.down_bytes
                measure = "bytes"
            else:
                category = "call"
                usage_type = row.data_type
                total = row.call_duration_sec
                measure = "seconds"
            
            usage_data.append({
                "category": category,
                "usage_type": usage_type,
                "total": total,
                "measure": measure,
                "start_time": f"{row.date} {row.hour:02d}:00:00"
            })
        
        return {
            "msisdn": msisdn,
            "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "usage": usage_data
        }, None

# Initialize API
scylla_hosts = os.getenv('SCYLLA_HOSTS', 'localhost').split(',')
api = UsageAPI(scylla_hosts)

@app.route('/data_usage', methods=['GET'])
@auth.login_required
def get_data_usage():
    """
    GET /data_usage?msisdn=2712345678&start_time=20240101010203&end_time=20240101235959
    """
    msisdn = request.args.get('msisdn')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    
    # Validate parameters
    if not all([msisdn, start_time, end_time]):
        return jsonify({
            "error": "Missing required parameters: msisdn, start_time, end_time"
        }), 400
    
    # Query data
    result, error = api.get_usage(msisdn, start_time, end_time)
    
    if error:
        return jsonify({"error": error}), 400
    
    return jsonify(result), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', '18089'))
    app.run(host='0.0.0.0', port=port, debug=False)