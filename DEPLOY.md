# PI Analytics Worker - Deployment Instructions

## Quick Deploy (Dashboard)

1. Go to https://dash.cloudflare.com > Workers & Pages > Create
2. Click "Create Worker"
3. Name it `pi-analytics`
4. Click "Deploy" then "Edit Code"
5. Replace contents with the bundled `pi-analytics-worker.js` file
6. Go to Settings > Variables and Secrets
7. Add these secrets:
   - `SUPABASE_URL` = `https://sfbfvmmgvcyiydioqnkr.supabase.co`
   - `SUPABASE_KEY` = [your Supabase anon key]
8. Deploy

## Endpoints

Once deployed, your worker will be available at:
`https://pi-analytics.<your-subdomain>.workers.dev`

### Available Endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /` | Service info and endpoint list |
| `GET /analytics/summary` | High-level dashboard metrics |
| `GET /analytics/pipeline` | Pipeline and stage analysis |
| `GET /analytics/pipeline?pipeline_id=X` | Specific pipeline |
| `GET /analytics/leads` | Lead quality and conversion |
| `GET /analytics/attribution` | Marketing attribution |
| `GET /analytics/data-quality` | Data quality issues |
| `GET /analytics/migration` | Salesforce migration status |
| `GET /analytics/opportunities` | Opportunity listing |
| `GET /analytics/contacts` | Contact listing |
| `GET /tables` | List all Supabase tables |
| `POST /query` | Custom queries |

### Query Parameters

**Opportunities:**
- `?status=open|won|lost`
- `?pipeline=PIPELINE_ID`
- `?limit=100` (max 500)
- `?offset=0`

**Contacts:**
- `?tag=pi-qualified`
- `?source=Qualification%20Survey`
- `?limit=100` (max 500)
- `?offset=0`

### Custom Query (POST /query)

```json
{
  "table": "opportunities",
  "select": "id,name,status,monetary_value",
  "filters": {
    "status": "eq.open"
  },
  "order": "ghl_created_at.desc",
  "limit": 50,
  "offset": 0
}
```

## Usage from Claude

Instead of running Python scripts, I can now call:

```bash
curl https://pi-analytics.<subdomain>.workers.dev/analytics/summary
```

This returns pre-computed analytics instantly without timeout risk.

## Wrangler Deploy (Alternative)

If you have wrangler configured:

```bash
cd analytics-worker
npm install
npx wrangler secret put SUPABASE_URL
npx wrangler secret put SUPABASE_KEY
npx wrangler deploy
```
