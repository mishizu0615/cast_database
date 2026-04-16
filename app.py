import os
import json
import random
import logging
from datetime import datetime
from flask import Flask, request, abort
import anthropic
import gspread
from google.oauth2.service_account import Credentials
import requests

# ========================================
# 初期設定
# ========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 環境変数
LINE_TOKEN      = os.environ['LINE_CHANNEL_ACCESS_TOKEN']
CLAUDE_API_KEY  = os.environ['CLAUDE_API_KEY']
SPREADSHEET_ID  = os.environ['SPREADSHEET_ID']
CREDENTIALS_JSON = os.environ['GOOGLE_CREDENTIALS_JSON']  # サービスアカウントのJSONを文字列で

CLAUDE_MODEL = 'claude-sonnet-4-20250514'

SCHEMA = [
    'staff_id', 'service', 'status', 'created_at', 'updated_at',
    'name', 'age', 'hometown', 'height', 'style', 'type', 'photo_url',
    'personality', 'hobbies', 'skills', 'profile_text', 'raw_memo',
    'transportation_fee', 'dormitory_fee', 'miscellaneous_fee',
    'is_home', 'is_biz_hotel',
]
BOOLEAN_COLS = {'is_home', 'is_biz_hotel'}

# ========================================
# Google Sheets 接続
# ========================================
def get_sheets_client():
    creds_dict = json.loads(CREDENTIALS_JSON)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def get_sheet(name='staff_db'):
    client = get_sheets_client()
    ss     = client.open_by_key(SPREADSHEET_ID)
    try:
        return ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=1000, cols=len(SCHEMA))
        ws.append_row(SCHEMA)
        ws.freeze(rows=1)
        return ws

def get_samples_sheet():
    client = get_sheets_client()
    ss     = client.open_by_key(SPREADSHEET_ID)
    try:
        return ss.worksheet('profile_samples')
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title='profile_samples', rows=500, cols=3)
        ws.append_row(['profile_text', 'added_at', 'memo'])
        ws.freeze(rows=1)
        return ws

# ========================================
# スタッフDB操作
# ========================================
def get_all_staff():
    ws   = get_sheet()
    rows = ws.get_all_records()
    return [r for r in rows if r.get('status') != '退職']

def find_staff_by_name(name):
    if not name or not name.strip():
        return None
    ws   = get_sheet()
    rows = ws.get_all_records()
    for i, r in enumerate(rows):
        row_name = str(r.get('name', '')).strip()
        if not row_name:
            continue  # 空行はスキップ
        if row_name == name.strip():
            logger.info(f'スタッフ発見: {row_name} row={i + 2}')
            return {'row_index': i + 2, 'data': r}
    logger.info(f'スタッフ未発見: {name} → 新規登録へ')
    return None

def generate_staff_id():
    ws   = get_sheet()
    rows = ws.get_all_records()
    ids  = [r.get('staff_id', '') for r in rows if r.get('staff_id')]
    nums = []
    for sid in ids:
        try:
            nums.append(int(str(sid).replace('STAFF_', '')))
        except:
            pass
    nxt = max(nums) + 1 if nums else 1
    return f'STAFF_{nxt:03d}'

def insert_staff(fields):
    ws       = get_sheet()
    now      = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    staff_id = generate_staff_id()
    row = []
    for col in SCHEMA:
        if col == 'staff_id':   row.append(staff_id)
        elif col == 'status':   row.append(fields.get('status', '在籍中'))
        elif col == 'created_at': row.append(now)
        elif col == 'updated_at': row.append(now)
        elif col in BOOLEAN_COLS: row.append(bool(fields.get(col, False)))
        else: row.append(fields.get(col, ''))
    logger.info(f'Sheets書き込み（新規）: {staff_id} | row={row[:6]}')
    ws.append_row(row)
    logger.info(f'Sheets書き込み完了: {staff_id}')
    return staff_id

def update_staff(row_index, fields):
    ws  = get_sheet()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated_at_col = SCHEMA.index('updated_at') + 1
    ws.update_cell(row_index, updated_at_col, now)

    for col, val in fields.items():
        if col not in SCHEMA:
            continue
        col_index = SCHEMA.index(col) + 1
        if col == 'raw_memo':
            existing = ws.cell(row_index, col_index).value or ''
            val = (existing + '\n---\n' + str(val)) if existing else str(val)
        ws.update_cell(row_index, col_index, val)

# ========================================
# プロフィール生成（学習ベース）
# ========================================
def collect_samples(exclude_name=''):
    samples = []

    # ① staff_db の profile_text
    for s in get_all_staff():
        pt = str(s.get('profile_text', '')).strip()
        if pt and len(pt) > 20 and s.get('name') != exclude_name:
            samples.append(pt)

    # ② profile_samples シート
    try:
        ws   = get_samples_sheet()
        rows = ws.get_all_records()
        for r in rows:
            pt = str(r.get('profile_text', '')).strip()
            if pt and len(pt) > 20:
                samples.append(pt)
    except Exception as e:
        logger.warning(f'profile_samples読み込みエラー: {e}')

    # 重複除去・シャッフル・最大20件
    seen, unique = set(), []
    for s in samples:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    random.shuffle(unique)
    return unique[:20]

def generate_profile_text(fields):
    name    = fields.get('name', '')
    samples = collect_samples(exclude_name=name)

    if not name:
        return ''

    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

    if samples:
        sample_block = '\n'.join(['---\n' + s for s in samples])
        system = (
            'あなたはプロフィール文の生成AIです。\n'
            '以下の【実例集】だけを手がかりに、同じ世界観・トーン・リズム・語彙感覚で'
            '新しいプロフィール文を1つ書いてください。\n'
            'ルールや制約は一切ありません。\n'
            '実例の空気感を最大限に吸収して、自然に書いてください。\n'
            'プロフィール文の本文だけ出力してください。説明や見出しは不要です。\n\n'
            '【実例集】\n' + sample_block
        )
    else:
        system = '自然で温かみのある日本語でスタッフの紹介文を200文字前後で書いてください。紹介文だけ出力してください。'

    info_lines = []
    mapping = {
        '名前': 'name', '年齢': 'age', '出身地': 'hometown',
        '身長': 'height', '雰囲気': 'type', 'スタイル': 'style',
        '性格': 'personality', '趣味': 'hobbies', '特技': 'skills',
    }
    for label, key in mapping.items():
        val = fields.get(key)
        if val:
            suffix = 'cm' if key == 'height' else ('歳' if key == 'age' else '')
            info_lines.append(f'{label}：{val}{suffix}')

    raw = fields.get('raw_memo', '')
    if raw:
        latest = raw.split('\n---\n')[-1].strip()
        if latest:
            info_lines.append(f'メモ：{latest}')

    user_msg = '上記の実例集の世界観で、以下のスタッフのプロフィール文を書いてください。\n\n' + '\n'.join(info_lines)

    res = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        system=system,
        messages=[{'role': 'user', 'content': user_msg}],
    )
    result = res.content[0].text.strip()
    logger.info(f'プロフィール生成: {name} | {len(result)}文字 | サンプル{len(samples)}件')
    return result

# ========================================
# 意図判定（Claude）
# ========================================
def detect_intent(text, all_staff):
    client    = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    staff_list = ', '.join([f"{s['name']}({s['staff_id']})" for s in all_staff]) or 'なし'

    system = f"""あなたはスタッフDBの管理AIです。
受け取ったメッセージの意図を判定し、JSON形式のみで返答してください。
余分なテキストや```json```は不要です。JSONのみ出力してください。

現在登録済みのスタッフ: {staff_list}

判定ルール:
- スタッフの情報（名前・年齢・趣味・交通費・寮費・雑費など）が含まれている → action: "register"
- 特定スタッフについて質問している → action: "query"
- プロフィール文の生成を求めている → action: "generate_profile"
- それ以外 → action: "unknown"

費用項目の抽出ルール:
- 「交通費◯円」「交通費◯」→ transportation_fee に数値で入れる
- 「寮費◯円」「寮費◯」→ dormitory_fee に数値で入れる
- 「雑費◯円」「雑費◯」→ miscellaneous_fee に数値で入れる
- 金額が不明な場合はそのままテキストで入れる"

返すJSONの形式:
{{
  "action": "register" | "query" | "generate_profile" | "unknown",
  "staff_name": "対象スタッフ名（わかれば）",
  "staff_id": "対象staff_id（わかれば）",
  "extracted_fields": {{
    "name": "", "age": null, "hometown": "", "height": null,
    "style": "", "type": "", "personality": "", "hobbies": "", "skills": "", "service": "",
    "transportation_fee": null, "dormitory_fee": null, "miscellaneous_fee": null
  }},
  "query_question": "質問内容（queryの場合）"
}}

extracted_fieldsは含まれていない項目は空文字列またはnullにしてください。"""

    res  = client.messages.create(
        model=CLAUDE_MODEL, max_tokens=512,
        system=system,
        messages=[{'role': 'user', 'content': text}],
    )
    try:
        return json.loads(res.content[0].text)
    except:
        logger.error(f'Intent parse error: {res.content[0].text}')
        return {'action': 'unknown'}

# ========================================
# LINE 返信
# ========================================
def reply_line(reply_token, message):
    truncated = message[:4990] + '…' if len(message) > 4990 else message
    requests.post(
        'https://api.line.me/v2/bot/message/reply',
        headers={'Authorization': f'Bearer {LINE_TOKEN}', 'Content-Type': 'application/json'},
        json={'replyToken': reply_token, 'messages': [{'type': 'text', 'text': truncated}]},
    )

def push_line_group(group_id, message):
    truncated = message[:4990] + '…' if len(message) > 4990 else message
    requests.post(
        'https://api.line.me/v2/bot/message/push',
        headers={'Authorization': f'Bearer {LINE_TOKEN}', 'Content-Type': 'application/json'},
        json={'to': group_id, 'messages': [{'type': 'text', 'text': truncated}]},
    )

def handle_staff_detail_(reply_token, name):
    existing = find_staff_by_name(name)
    if not existing:
        reply_line(reply_token, f'「{name}」は見つかりませんでした。
名前が完全一致している必要があります。')
        return
    d = existing['data']

    def val(v):
        return str(v) if v not in ('', None, False) else '未登録'

    lines = [
        f'【{d.get("name", "")}】{d.get("staff_id", "")}',
        f'ステータス：{val(d.get("status"))}',
        f'サービス：{val(d.get("service"))}',
        '',
        f'年齢：{val(d.get("age"))}歳' if d.get("age") else f'年齢：未登録',
        f'出身地：{val(d.get("hometown"))}',
        f'身長：{str(d.get("height")) + "cm" if d.get("height") else "未登録"}',
        f'タイプ：{val(d.get("type"))}',
        f'スタイル：{val(d.get("style"))}',
        '',
        f'性格：{val(d.get("personality"))}',
        f'趣味：{val(d.get("hobbies"))}',
        f'特技：{val(d.get("skills"))}',
        '',
        f'交通費：{val(d.get("transportation_fee"))}',
        f'寮費：{val(d.get("dormitory_fee"))}',
        f'雑費：{val(d.get("miscellaneous_fee"))}',
        f'自宅：{"あり" if d.get("is_home") else "なし"}',
        f'ビジホ：{"あり" if d.get("is_biz_hotel") else "なし"}',
        '',
        f'プロフィール文：{"あり" if d.get("profile_text") else "未生成"}',
        f'登録日：{str(d.get("created_at", ""))[:10]}',
        f'更新日：{str(d.get("updated_at", ""))[:10]}',
    ]
    reply_line(reply_token, '
'.join(lines))

def get_help_text():
    return '\n'.join([
        '【スタッフDB 使い方】',
        '',
        '▼ 登録・更新',
        '「さくら、22歳、広島出身、趣味は映画」',
        '',
        '▼ 情報を呼び出す',
        '「さくらちゃんの趣味は？」',
        '',
        '▼ プロフィール生成',
        '「さくらのプロフィールを作って」',
        '',
        '▼ 一覧確認',
        '「スタッフ一覧」',
        '',
        '▼ 学習データ追加',
        '「サンプル登録：〈プロフィール文〉」',
        '',
        '▼ 学習データ確認',
        '「サンプル一覧」',
    ])

# ========================================
# メッセージ処理
# ========================================
def handle_text(reply_token, group_id, text):
    logger.info(f'受信: "{text}" from group={group_id}')

    # ① キーワードコマンド（Claude判定より優先）
    if text.startswith('サンプル登録：') or text.startswith('サンプル登録:'):
        body = text.split('：', 1)[-1].split(':', 1)[-1].strip()
        if len(body) < 10:
            reply_line(reply_token, 'プロフィール文が短すぎます。20文字以上で送ってください。')
            return
        ws  = get_samples_sheet()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ws.append_row([body, now, ''])
        total = len(ws.get_all_records())
        reply_line(reply_token, f'✅ サンプルを登録しました！\n現在の学習データ：{total}件\n\n【登録内容】\n{body}')
        return

    if text.strip() == 'サンプル一覧':
        samples = collect_samples()
        if not samples:
            reply_line(reply_token, 'まだ学習データがありません。')
            return
        preview = '\n'.join([f'【{i+1}】{s[:40]}…' for i, s in enumerate(samples[:5])])
        reply_line(reply_token, f'現在の学習データ：{len(samples)}件\n\n{preview}')
        return

    if text.strip() == 'スタッフ一覧':
        all_staff = get_all_staff()
        if not all_staff:
            reply_line(reply_token, 'まだスタッフが登録されていません。')
            return
        lines = []
        for s in all_staff:
            has_profile = '✅' if s.get('profile_text') else '　'
            lines.append(f"{has_profile} {s['name']}（{s.get('age', '?')}歳）")
        reply_line(reply_token, f"【登録スタッフ一覧】{len(all_staff)}名\n\n" + '\n'.join(lines))
        return

    if text.strip() == '使い方':
        reply_line(reply_token, get_help_text())
        return

    # 「〇〇の情報」でスタッフ詳細を返す
    if text.strip().endswith('の情報'):
        name = text.strip()[:-3].strip()
        handle_staff_detail_(reply_token, name)
        return

    # ② Claude で意図判定
    all_staff = get_all_staff()
    intent    = detect_intent(text, all_staff)
    action    = intent.get('action', 'unknown')
    logger.info(f'Intent: {action}')

    if action == 'register':
        fields = intent.get('extracted_fields') or {}
        # 空文字・Noneを除去
        fields = {k: v for k, v in fields.items() if v not in ('', None)}
        fields['raw_memo'] = text

        staff_name = intent.get('staff_name') or fields.get('name', '')
        existing   = find_staff_by_name(staff_name) if staff_name else None

        # プロフィール生成はしない。データ蓄積のみ。
        if existing:
            update_staff(existing['row_index'], fields)
            logger.info(f'スタッフ更新: {staff_name} row={existing["row_index"]}')
            msg = f"✅ {staff_name} の情報を蓄積しました！\nプロフィールを生成する場合は「{staff_name}のプロフィールを作って」と送ってください。"
        else:
            staff_id = insert_staff(fields)
            name     = fields.get('name', '名前未設定')
            logger.info(f'スタッフ新規登録: {name} id={staff_id}')
            msg = f"✅ 新規スタッフ「{name}」を登録しました！\nID: {staff_id}\nプロフィールを生成する場合は「{name}のプロフィールを作って」と送ってください。"
        reply_line(reply_token, msg)

    elif action == 'query':
        staff_name = intent.get('staff_name', '')
        if staff_name:
            targets = [s for s in all_staff if staff_name in str(s.get('name', ''))]
        else:
            targets = all_staff
        context = json.dumps(targets or all_staff, ensure_ascii=False, indent=2)

        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
        res    = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=512,
            system=f'あなたはCo-Nookのスタッフ管理AIです。以下のDBをもとに質問に200文字以内で答えてください。\n\n【スタッフDB】\n{context}',
            messages=[{'role': 'user', 'content': intent.get('query_question') or text}],
        )
        reply_line(reply_token, res.content[0].text.strip())

    elif action == 'generate_profile':
        staff_name = intent.get('staff_name', '')
        existing   = find_staff_by_name(staff_name) if staff_name else None
        if not existing:
            reply_line(reply_token, f'{staff_name} が見つかりませんでした。')
            return
        fields       = existing['data']
        profile_text = generate_profile_text(fields)
        if not profile_text:
            reply_line(reply_token, 'プロフィール生成に失敗しました。')
            return
        update_staff(existing['row_index'], {'profile_text': profile_text})
        reply_line(reply_token, f'✅ プロフィール文を生成・保存しました！\n\n{profile_text}')

    else:
        # unknown はグループでは静かにスルー
        logger.info('unknown intent — スルー')

# ========================================
# Flask エンドポイント
# ========================================
@app.route('/webhook', methods=['POST'])
def webhook():
    body   = request.get_json(silent=True) or {}
    events = body.get('events', [])

    for event in events:
        if event.get('type') != 'message':
            continue

        source   = event.get('source', {})
        msg_type = event.get('message', {}).get('type')

        # グループ専用
        if source.get('type') != 'group':
            logger.info(f"グループ以外は無視: {source.get('type')}")
            continue

        reply_token = event.get('replyToken')
        group_id    = source.get('groupId')

        try:
            if msg_type == 'text':
                handle_text(reply_token, group_id, event['message']['text'])
            else:
                reply_line(reply_token, 'テキストを送ってね！')
        except Exception as err:
            logger.exception(f'Error: {err}')
            reply_line(reply_token, f'エラーが発生しました: {str(err)[:100]}')

    return 'OK', 200

@app.route('/health', methods=['GET'])
def health():
    return {'status': 'ok', 'timestamp': datetime.now().isoformat()}, 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
