#!/usr/bin/env python3
"""
External System Service - Webhook endpoint
Simule un système externe qui reçoit les événements enrichis
"""

from flask import Flask, request, jsonify
import json
from datetime import datetime
import os

app = Flask(__name__)

# Simple in-memory storage pour demo
received_events = []
MAX_EVENTS = 1000  # Keep last 1000 events

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Endpoint principal pour recevoir les événements du pipeline Flink
    """
    try:
        data = request.get_json()
        
        # Add timestamp
        data['received_at'] = datetime.now().isoformat()
        
        # Store event (keep only last MAX_EVENTS)
        received_events.append(data)
        if len(received_events) > MAX_EVENTS:
            received_events.pop(0)
        
        # Log for monitoring
        print(f"📨 Received event: user={data.get('user_id')}, content={data.get('content_id')}, engagement={data.get('engagement_pct', 0):.1%}")
        
        return jsonify({
            'status': 'success',
            'message': 'Event received',
            'event_id': data.get('id')
        }), 200
        
    except Exception as e:
        print(f"❌ Webhook error: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'events_received': len(received_events),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/events', methods=['GET'])
def get_events():
    """
    Get recent events for monitoring/debugging
    """
    limit = int(request.args.get('limit', 10))
    return jsonify({
        'events': received_events[-limit:],
        'total_count': len(received_events)
    })

@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Get basic statistics about received events
    """
    if not received_events:
        return jsonify({'message': 'No events received yet'})
    
    # Basic stats
    content_types = {}
    engagement_sum = 0
    engagement_count = 0
    
    for event in received_events[-100:]:  # Last 100 events
        # Content type distribution
        ct = event.get('content_type')
        if ct:
            content_types[ct] = content_types.get(ct, 0) + 1
        
        # Average engagement
        eng_pct = event.get('engagement_pct')
        if eng_pct is not None:
            engagement_sum += eng_pct
            engagement_count += 1
    
    return jsonify({
        'total_events': len(received_events),
        'content_type_distribution': content_types,
        'average_engagement': engagement_sum / engagement_count if engagement_count > 0 else 0,
        'last_updated': datetime.now().isoformat()
    })

@app.route('/', methods=['GET'])
def index():
    """
    Simple HTML dashboard for monitoring
    """
    return f"""
    <html>
    <head><title>External System - Event Receiver</title></head>
    <body style="font-family: Arial; margin: 40px;">
        <h1>🌐 External System Dashboard</h1>
        <div style="background: #f0f0f0; padding: 20px; border-radius: 8px;">
            <h2>Status</h2>
            <p>✅ Service is running</p>
            <p>📊 Events received: <strong>{len(received_events)}</strong></p>
        </div>
        
        <div style="margin-top: 20px;">
            <h2>API Endpoints</h2>
            <ul>
                <li><a href="/health">GET /health</a> - Health check</li>
                <li><a href="/events?limit=5">GET /events</a> - Recent events</li>
                <li><a href="/stats">GET /stats</a> - Statistics</li>
                <li>POST /webhook - Receive events</li>
            </ul>
        </div>
        
        <div style="margin-top: 20px;">
            <h2>Recent Events</h2>
            <pre style="background: #f8f8f8; padding: 10px; border-radius: 4px; overflow-x: auto;">
{json.dumps(received_events[-3:], indent=2, default=str)}
            </pre>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)