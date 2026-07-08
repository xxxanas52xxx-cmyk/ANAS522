"""
Quotex Charts — Flask API server (v4 — بث فوري + تبديل أزواج بدون تأخير)
"""
import time
import json
import threading
from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context
from flask_cors import CORS
import bot

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

_start_lock = threading.Lock()

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/start', methods=['POST'])
def api_start():
    data  = request.get_json(silent=True) or {}
    asset = data.get('asset', 'NZDUSD_otc')
    with _start_lock:
        bot.start_service(asset)
    # إن كانت الخدمة تعمل بالفعل (تبديل زوج)، هذا يرجع فورًا تقريبًا
    deadline = time.time() + 15
    while time.time() < deadline:
        if bot.is_connected():
            return jsonify({'status': 'ok', 'asset': asset})
        err = bot.get_asset_error(asset)
        if err:
            return jsonify({'status': 'error', 'asset': asset, 'message': err['error']}), 200
        time.sleep(0.2)
    return jsonify({'status': 'connecting', 'asset': asset,
                    'message': 'جارٍ الاتصال…'}), 202

@app.route('/api/seed_asset', methods=['POST'])
def api_seed_asset():
    """
    يضيف زوجًا للمراقبة فورًا (بدون قطع الاتصال الحالي بأي زوج آخر).
    هذا ما يجعل تبديل الزوج في الواجهة فوريًا تقريبًا بدل 15-30 ثانية.
    """
    data  = request.get_json(silent=True) or {}
    asset = data.get('asset', 'NZDUSD_otc')
    if not bot.is_connected():
        return jsonify({'status': 'not_connected'}), 200
    bot.watch_asset(asset)
    return jsonify({'status': 'ok', 'asset': asset})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    bot.stop_service()
    return jsonify({'status': 'ok'})

@app.route('/api/candles')
def api_candles():
    asset = request.args.get('asset', 'NZDUSD_otc')
    tf    = float(request.args.get('tf', 1))
    return jsonify({
        'status': 'ok', 'asset': asset, 'tf': tf,
        'candles': bot.get_candles(asset, tf)
    })

@app.route('/api/history')
def api_history():
    asset   = request.args.get('asset', 'NZDUSD_otc')
    tf      = float(request.args.get('tf', 1))
    limit   = int(request.args.get('limit', 0))
    candles = bot.get_history(asset, tf)
    if limit > 0:
        candles = candles[-limit:]
    return jsonify({
        'status': 'ok', 'asset': asset, 'tf': tf,
        'count': len(candles), 'candles': candles
    })

@app.route('/api/history_status')
def api_history_status():
    asset  = request.args.get('asset', 'NZDUSD_otc')
    status = bot.get_history_status(asset)
    return jsonify({'status': 'ok', 'asset': asset, 'timeframes': status})

@app.route('/api/price')
def api_price():
    asset = request.args.get('asset', 'NZDUSD_otc')
    live  = bot.get_live_price(asset)
    if live is None:
        return jsonify({'status': 'waiting', 'price': None, 'time': None})
    return jsonify({'status': 'ok', 'price': live['price'], 'time': live['time']})

@app.route('/api/stream')
def api_stream():
    """
    SSE — بث فوري للتيكات (يستبدل الـ polling البطيء بالكامل).
    كل تيك يصل من Quotex يُدفع مباشرة للمتصفح خلال أجزاء من الثانية،
    بدل انتظار دورة polling التالية.
    """
    asset = request.args.get('asset', 'NZDUSD_otc')
    bot.watch_asset(asset)

    def gen():
        q = bot.sse_subscribe(asset)
        try:
            last_ping = time.time()
            while True:
                if q:
                    payload = q.popleft()
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_ping = time.time()
                else:
                    time.sleep(0.05)
                    if time.time() - last_ping > 10:
                        yield "data: {\"ping\": true}\n\n"
                        last_ping = time.time()
        except GeneratorExit:
            pass
        finally:
            bot.sse_unsubscribe(asset, q)

    return Response(stream_with_context(gen()), mimetype='text/event-stream',
                     headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/asset_error')
def api_asset_error():
    """تشخيص: لماذا لا يعمل زوج معيّن (مثل USD/BRL OTC)؟"""
    asset = request.args.get('asset', 'NZDUSD_otc')
    err   = bot.get_asset_error(asset)
    if err:
        return jsonify({'status': 'error', 'asset': asset, **err})
    return jsonify({'status': 'ok', 'asset': asset, 'error': None})

@app.route('/api/status')
def api_status():
    asset = request.args.get('asset', 'NZDUSD_otc')
    live  = bot.get_live_price(asset)
    conn  = bot.get_conn_status()
    err   = bot.get_asset_error(asset)
    return jsonify({
        'connected':    bot.is_connected(),
        'has_data':     live is not None,
        'price':        live['price'] if live else None,
        'conn_state':   conn.get('state', 'unknown'),
        'conn_message': conn.get('message', ''),
        'conn_attempt': conn.get('attempt', 0),
        'asset_error':  err['error'] if err else None,
    })

@app.route('/api/bulk_fetch', methods=['POST'])
def api_bulk_fetch():
    data       = request.get_json(silent=True) or {}
    asset      = data.get('asset', 'NZDUSD_otc')
    tf         = float(data.get('tf', 1))
    period_sec = bot._tf_to_period(tf)
    result     = bot.trigger_bulk_fetch(asset, period_sec)
    return jsonify(result)

@app.route('/api/bulk_status')
def api_bulk_status():
    asset      = request.args.get('asset', 'NZDUSD_otc')
    tf         = float(request.args.get('tf', 1))
    period_sec = bot._tf_to_period(tf)
    status     = bot.get_bulk_status(asset, period_sec)
    status['db_count'] = bot._db_count(asset, period_sec)
    return jsonify({'status': 'ok', **status})

if __name__ == '__main__':
    print('\n  QUOTEX — LEADERS OF TRADING')
    print('  Charts server  →  http://localhost:5000\n')
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)