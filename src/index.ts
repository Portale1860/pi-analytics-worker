/**
 * PI Analytics Worker
 * Comprehensive analytics service for Supabase data
 * Handles heavy-lifting queries server-side to prevent client timeouts
 */

interface Env {
  SUPABASE_URL: string;
  SUPABASE_KEY: string;
}

interface SupabaseResponse<T> {
  data: T[];
  error?: { message: string };
}

// CORS headers for all responses
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

// Helper to make Supabase REST API calls
async function supabaseQuery<T>(
  env: Env,
  table: string,
  params: Record<string, string> = {}
): Promise<T[]> {
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/${table}`);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.set(key, value);
  });

  const response = await fetch(url.toString(), {
    headers: {
      apikey: env.SUPABASE_KEY,
      Authorization: `Bearer ${env.SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=representation',
    },
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Supabase error: ${response.status} - ${error}`);
  }

  return response.json();
}

// Helper for RPC calls (stored procedures)
async function supabaseRpc<T>(
  env: Env,
  functionName: string,
  params: Record<string, unknown> = {}
): Promise<T> {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${functionName}`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_KEY,
      Authorization: `Bearer ${env.SUPABASE_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(params),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Supabase RPC error: ${response.status} - ${error}`);
  }

  return response.json();
}

// JSON response helper
function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders,
    },
  });
}

// Error response helper
function errorResponse(message: string, status = 500): Response {
  return jsonResponse({ error: message, timestamp: new Date().toISOString() }, status);
}

// ============================================================================
// ANALYTICS ENDPOINTS
// ============================================================================

/**
 * GET /analytics/summary
 * High-level dashboard metrics
 */
async function getSummary(env: Env): Promise<Response> {
  try {
    const [opportunities, contacts, pipelineStages] = await Promise.all([
      supabaseQuery<any>(env, 'opportunities', { select: 'id,status,monetary_value,pipeline_id,pipeline_stage_id,ghl_created_at' }),
      supabaseQuery<any>(env, 'contacts', { select: 'id,tags,source,created_at' }),
      supabaseQuery<any>(env, 'pipeline_stages', { select: '*' }),
    ]);

    // Opportunity metrics
    const oppByStatus = opportunities.reduce((acc: Record<string, number>, opp: any) => {
      acc[opp.status] = (acc[opp.status] || 0) + 1;
      return acc;
    }, {});

    const totalValue = opportunities.reduce((sum: number, opp: any) => sum + (opp.monetary_value || 0), 0);

    // Contact metrics
    const qualifiedContacts = contacts.filter((c: any) => c.tags?.includes('pi-qualified')).length;
    const disqualifiedContacts = contacts.filter((c: any) => c.tags?.includes('pi-disqualified')).length;

    // Pipeline breakdown
    const stageMap = new Map(pipelineStages.map((s: any) => [s.stage_id, s]));
    const oppByStage = opportunities.reduce((acc: Record<string, { count: number; value: number; stage_name: string; pipeline_name: string }>, opp: any) => {
      const stageId = opp.pipeline_stage_id;
      const stage = stageMap.get(stageId);
      if (stage) {
        const key = `${stage.pipeline_name}|${stage.stage_name}`;
        if (!acc[key]) {
          acc[key] = { count: 0, value: 0, stage_name: stage.stage_name, pipeline_name: stage.pipeline_name };
        }
        acc[key].count++;
        acc[key].value += opp.monetary_value || 0;
      }
      return acc;
    }, {});

    // Time-based metrics (last 30 days)
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const recentOpps = opportunities.filter((o: any) => new Date(o.ghl_created_at) > thirtyDaysAgo);
    const recentContacts = contacts.filter((c: any) => new Date(c.created_at) > thirtyDaysAgo);

    return jsonResponse({
      timestamp: new Date().toISOString(),
      summary: {
        total_opportunities: opportunities.length,
        total_contacts: contacts.length,
        total_pipeline_value: totalValue,
        opportunities_by_status: oppByStatus,
        qualified_contacts: qualifiedContacts,
        disqualified_contacts: disqualifiedContacts,
        qualification_rate: contacts.length > 0 ? ((qualifiedContacts / contacts.length) * 100).toFixed(1) + '%' : 'N/A',
      },
      last_30_days: {
        new_opportunities: recentOpps.length,
        new_contacts: recentContacts.length,
      },
      pipeline_breakdown: Object.values(oppByStage).sort((a: any, b: any) => b.count - a.count),
    });
  } catch (error) {
    return errorResponse(`Summary error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/pipeline
 * Detailed pipeline and stage analysis
 */
async function getPipelineAnalytics(env: Env, pipelineId?: string): Promise<Response> {
  try {
    const [opportunities, pipelineStages] = await Promise.all([
      supabaseQuery<any>(env, 'opportunities', {
        select: 'id,name,status,monetary_value,pipeline_id,pipeline_stage_id,ghl_created_at,last_stage_change_at,contact',
        ...(pipelineId ? { pipeline_id: `eq.${pipelineId}` } : {}),
      }),
      supabaseQuery<any>(env, 'pipeline_stages', { select: '*', order: 'pipeline_name,stage_position' }),
    ]);

    // Group stages by pipeline
    const pipelines: Record<string, { name: string; stages: any[]; opportunities: any[] }> = {};

    pipelineStages.forEach((stage: any) => {
      if (!pipelines[stage.ghl_pipeline_id]) {
        pipelines[stage.ghl_pipeline_id] = {
          name: stage.pipeline_name,
          stages: [],
          opportunities: [],
        };
      }
      pipelines[stage.ghl_pipeline_id].stages.push({
        id: stage.stage_id,
        name: stage.stage_name,
        position: stage.stage_position,
        count: 0,
        value: 0,
        opportunities: [],
      });
    });

    // Assign opportunities to stages
    opportunities.forEach((opp: any) => {
      const pipeline = pipelines[opp.pipeline_id];
      if (pipeline) {
        pipeline.opportunities.push(opp);
        const stage = pipeline.stages.find((s: any) => s.id === opp.pipeline_stage_id);
        if (stage) {
          stage.count++;
          stage.value += opp.monetary_value || 0;
          stage.opportunities.push({
            id: opp.id,
            name: opp.name,
            value: opp.monetary_value,
            status: opp.status,
            created: opp.ghl_created_at,
            last_stage_change: opp.last_stage_change_at,
            contact_email: opp.contact?.email,
          });
        }
      }
    });

    // Calculate conversion rates between stages
    Object.values(pipelines).forEach((pipeline: any) => {
      pipeline.stages.sort((a: any, b: any) => a.position - b.position);
      for (let i = 1; i < pipeline.stages.length; i++) {
        const prevCount = pipeline.stages[i - 1].count;
        const currCount = pipeline.stages[i].count;
        pipeline.stages[i].conversion_from_previous = prevCount > 0
          ? ((currCount / prevCount) * 100).toFixed(1) + '%'
          : 'N/A';
      }
      pipeline.total_opportunities = pipeline.opportunities.length;
      pipeline.total_value = pipeline.opportunities.reduce((sum: number, o: any) => sum + (o.monetary_value || 0), 0);
      delete pipeline.opportunities; // Remove raw data to reduce response size
    });

    return jsonResponse({
      timestamp: new Date().toISOString(),
      pipelines,
    });
  } catch (error) {
    return errorResponse(`Pipeline error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/leads
 * Lead quality and conversion analysis
 */
async function getLeadAnalytics(env: Env): Promise<Response> {
  try {
    const contacts = await supabaseQuery<any>(env, 'contacts', {
      select: 'id,tags,source,email,phone,created_at,custom_fields',
    });

    // Categorize by tags
    const qualified = contacts.filter((c: any) => c.tags?.includes('pi-qualified'));
    const disqualified = contacts.filter((c: any) => c.tags?.includes('pi-disqualified'));
    const uncategorized = contacts.filter((c: any) =>
      !c.tags?.includes('pi-qualified') && !c.tags?.includes('pi-disqualified')
    );

    // Source breakdown
    const bySource = contacts.reduce((acc: Record<string, { total: number; qualified: number; disqualified: number }>, c: any) => {
      const source = c.source || 'Unknown';
      if (!acc[source]) {
        acc[source] = { total: 0, qualified: 0, disqualified: 0 };
      }
      acc[source].total++;
      if (c.tags?.includes('pi-qualified')) acc[source].qualified++;
      if (c.tags?.includes('pi-disqualified')) acc[source].disqualified++;
      return acc;
    }, {});

    // Tag frequency
    const tagCounts = contacts.reduce((acc: Record<string, number>, c: any) => {
      (c.tags || []).forEach((tag: string) => {
        acc[tag] = (acc[tag] || 0) + 1;
      });
      return acc;
    }, {});

    // Data quality
    const withEmail = contacts.filter((c: any) => c.email).length;
    const withPhone = contacts.filter((c: any) => c.phone).length;
    const withBoth = contacts.filter((c: any) => c.email && c.phone).length;

    // Time series (by week)
    const byWeek = contacts.reduce((acc: Record<string, { total: number; qualified: number }>, c: any) => {
      const date = new Date(c.created_at);
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - date.getDay());
      const key = weekStart.toISOString().split('T')[0];
      if (!acc[key]) {
        acc[key] = { total: 0, qualified: 0 };
      }
      acc[key].total++;
      if (c.tags?.includes('pi-qualified')) acc[key].qualified++;
      return acc;
    }, {});

    return jsonResponse({
      timestamp: new Date().toISOString(),
      totals: {
        total_contacts: contacts.length,
        qualified: qualified.length,
        disqualified: disqualified.length,
        uncategorized: uncategorized.length,
        qualification_rate: contacts.length > 0 ? ((qualified.length / contacts.length) * 100).toFixed(1) + '%' : 'N/A',
      },
      by_source: Object.entries(bySource)
        .map(([source, data]) => ({
          source,
          ...data,
          qualification_rate: data.total > 0 ? ((data.qualified / data.total) * 100).toFixed(1) + '%' : 'N/A',
        }))
        .sort((a, b) => b.total - a.total),
      tag_frequency: Object.entries(tagCounts)
        .map(([tag, count]) => ({ tag, count }))
        .sort((a, b) => b.count - a.count),
      data_quality: {
        with_email: withEmail,
        with_phone: withPhone,
        with_both: withBoth,
        email_rate: ((withEmail / contacts.length) * 100).toFixed(1) + '%',
        phone_rate: ((withPhone / contacts.length) * 100).toFixed(1) + '%',
      },
      weekly_trend: Object.entries(byWeek)
        .map(([week, data]) => ({ week, ...data }))
        .sort((a, b) => a.week.localeCompare(b.week))
        .slice(-12), // Last 12 weeks
    });
  } catch (error) {
    return errorResponse(`Lead analytics error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/attribution
 * Marketing attribution analysis
 */
async function getAttributionAnalytics(env: Env): Promise<Response> {
  try {
    const opportunities = await supabaseQuery<any>(env, 'opportunities', {
      select: 'id,name,status,monetary_value,attributions,source,ghl_created_at',
    });

    // First touch attribution
    const firstTouch: Record<string, { count: number; value: number; won: number }> = {};
    // Last touch attribution
    const lastTouch: Record<string, { count: number; value: number; won: number }> = {};
    // UTM sources
    const utmSources: Record<string, { count: number; value: number }> = {};
    // Medium breakdown
    const mediums: Record<string, { count: number; value: number }> = {};

    opportunities.forEach((opp: any) => {
      const attrs = opp.attributions || [];
      const value = opp.monetary_value || 0;
      const isWon = opp.status === 'won';

      // First touch
      const first = attrs.find((a: any) => a.isFirst);
      if (first) {
        const source = first.utmSessionSource || 'Direct';
        if (!firstTouch[source]) firstTouch[source] = { count: 0, value: 0, won: 0 };
        firstTouch[source].count++;
        firstTouch[source].value += value;
        if (isWon) firstTouch[source].won++;
      }

      // Last touch
      const last = attrs.find((a: any) => a.isLast);
      if (last) {
        const source = last.utmSessionSource || 'Direct';
        if (!lastTouch[source]) lastTouch[source] = { count: 0, value: 0, won: 0 };
        lastTouch[source].count++;
        lastTouch[source].value += value;
        if (isWon) lastTouch[source].won++;
      }

      // All UTM sources and mediums
      attrs.forEach((attr: any) => {
        const utmSource = attr.utmSessionSource || 'Direct';
        if (!utmSources[utmSource]) utmSources[utmSource] = { count: 0, value: 0 };
        utmSources[utmSource].count++;
        utmSources[utmSource].value += value;

        const medium = attr.medium || 'Unknown';
        if (!mediums[medium]) mediums[medium] = { count: 0, value: 0 };
        mediums[medium].count++;
        mediums[medium].value += value;
      });
    });

    return jsonResponse({
      timestamp: new Date().toISOString(),
      total_opportunities: opportunities.length,
      first_touch_attribution: Object.entries(firstTouch)
        .map(([source, data]) => ({ source, ...data }))
        .sort((a, b) => b.count - a.count),
      last_touch_attribution: Object.entries(lastTouch)
        .map(([source, data]) => ({ source, ...data }))
        .sort((a, b) => b.count - a.count),
      utm_sources: Object.entries(utmSources)
        .map(([source, data]) => ({ source, ...data }))
        .sort((a, b) => b.count - a.count),
      mediums: Object.entries(mediums)
        .map(([medium, data]) => ({ medium, ...data }))
        .sort((a, b) => b.count - a.count),
    });
  } catch (error) {
    return errorResponse(`Attribution error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/data-quality
 * Data quality analysis and issues
 */
async function getDataQualityAnalytics(env: Env): Promise<Response> {
  try {
    const [contacts, opportunities, dataQualityIssues] = await Promise.all([
      supabaseQuery<any>(env, 'contacts', { select: 'id,email,phone,tags,source,first_name,last_name' }),
      supabaseQuery<any>(env, 'opportunities', { select: 'id,name,ghl_contact_id,contact,monetary_value,status' }),
      supabaseQuery<any>(env, 'data_quality_issues', { select: '*' }).catch(() => []),
    ]);

    // Contact issues
    const contactIssues = {
      missing_email: contacts.filter((c: any) => !c.email).length,
      missing_phone: contacts.filter((c: any) => !c.phone).length,
      missing_name: contacts.filter((c: any) => !c.first_name && !c.last_name).length,
      no_tags: contacts.filter((c: any) => !c.tags || c.tags.length === 0).length,
      no_source: contacts.filter((c: any) => !c.source).length,
    };

    // Opportunity issues
    const oppIssues = {
      missing_value: opportunities.filter((o: any) => !o.monetary_value || o.monetary_value === 0).length,
      missing_contact: opportunities.filter((o: any) => !o.ghl_contact_id).length,
      no_status: opportunities.filter((o: any) => !o.status).length,
    };

    // Duplicate detection (by email)
    const emailCounts = contacts.reduce((acc: Record<string, number>, c: any) => {
      if (c.email) {
        const email = c.email.toLowerCase();
        acc[email] = (acc[email] || 0) + 1;
      }
      return acc;
    }, {});
    const duplicateEmails = Object.entries(emailCounts)
      .filter(([_, count]) => count > 1)
      .map(([email, count]) => ({ email, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 20);

    // Phone duplicate detection
    const phoneCounts = contacts.reduce((acc: Record<string, number>, c: any) => {
      if (c.phone) {
        acc[c.phone] = (acc[c.phone] || 0) + 1;
      }
      return acc;
    }, {});
    const duplicatePhones = Object.entries(phoneCounts)
      .filter(([_, count]) => count > 1)
      .map(([phone, count]) => ({ phone, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 20);

    // Calculate completeness scores
    const contactCompleteness = contacts.length > 0
      ? (
          (contacts.filter((c: any) => c.email).length +
            contacts.filter((c: any) => c.phone).length +
            contacts.filter((c: any) => c.first_name || c.last_name).length +
            contacts.filter((c: any) => c.source).length) /
          (contacts.length * 4)
        ) * 100
      : 0;

    return jsonResponse({
      timestamp: new Date().toISOString(),
      totals: {
        total_contacts: contacts.length,
        total_opportunities: opportunities.length,
      },
      contact_issues: {
        ...contactIssues,
        completeness_score: contactCompleteness.toFixed(1) + '%',
      },
      opportunity_issues: oppIssues,
      duplicates: {
        duplicate_emails: duplicateEmails,
        duplicate_phones: duplicatePhones,
        total_duplicate_email_contacts: duplicateEmails.reduce((sum, d) => sum + d.count - 1, 0),
        total_duplicate_phone_contacts: duplicatePhones.reduce((sum, d) => sum + d.count - 1, 0),
      },
      stored_issues: dataQualityIssues.slice(0, 50),
    });
  } catch (error) {
    return errorResponse(`Data quality error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/migration
 * Compare data between GHL/Supabase and Salesforce
 */
async function getMigrationStatus(env: Env): Promise<Response> {
  try {
    const [contacts, sfContacts, opportunities, sfOpportunities, appointments, sfAppointments] = await Promise.all([
      supabaseQuery<any>(env, 'contacts', { select: 'id' }),
      supabaseQuery<any>(env, 'sf_contacts', { select: 'id,salesforce_id' }).catch(() => []),
      supabaseQuery<any>(env, 'opportunities', { select: 'id' }),
      supabaseQuery<any>(env, 'sf_opportunities', { select: 'id,salesforce_id' }).catch(() => []),
      supabaseQuery<any>(env, 'appointments', { select: 'id' }).catch(() => []),
      supabaseQuery<any>(env, 'sf_appointments', { select: 'id,salesforce_id' }).catch(() => []),
    ]);

    const sfContactsWithId = sfContacts.filter((c: any) => c.salesforce_id);
    const sfOppsWithId = sfOpportunities.filter((o: any) => o.salesforce_id);
    const sfApptsWithId = sfAppointments.filter((a: any) => a.salesforce_id);

    return jsonResponse({
      timestamp: new Date().toISOString(),
      migration_status: {
        contacts: {
          supabase_total: contacts.length,
          salesforce_staged: sfContacts.length,
          salesforce_synced: sfContactsWithId.length,
          pending_sync: sfContacts.length - sfContactsWithId.length,
          migration_rate: contacts.length > 0 ? ((sfContactsWithId.length / contacts.length) * 100).toFixed(1) + '%' : 'N/A',
        },
        opportunities: {
          supabase_total: opportunities.length,
          salesforce_staged: sfOpportunities.length,
          salesforce_synced: sfOppsWithId.length,
          pending_sync: sfOpportunities.length - sfOppsWithId.length,
          migration_rate: opportunities.length > 0 ? ((sfOppsWithId.length / opportunities.length) * 100).toFixed(1) + '%' : 'N/A',
        },
        appointments: {
          supabase_total: appointments.length,
          salesforce_staged: sfAppointments.length,
          salesforce_synced: sfApptsWithId.length,
          pending_sync: sfAppointments.length - sfApptsWithId.length,
          migration_rate: appointments.length > 0 ? ((sfApptsWithId.length / appointments.length) * 100).toFixed(1) + '%' : 'N/A',
        },
      },
    });
  } catch (error) {
    return errorResponse(`Migration status error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/opportunities
 * Detailed opportunity listing with filters
 */
async function getOpportunities(env: Env, url: URL): Promise<Response> {
  try {
    const status = url.searchParams.get('status');
    const pipeline = url.searchParams.get('pipeline');
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 500);
    const offset = parseInt(url.searchParams.get('offset') || '0');

    const params: Record<string, string> = {
      select: 'id,ghl_opportunity_id,name,status,monetary_value,source,pipeline_id,pipeline_stage_id,contact,attributions,ghl_created_at,last_stage_change_at',
      order: 'ghl_created_at.desc',
      limit: limit.toString(),
      offset: offset.toString(),
    };

    if (status) params['status'] = `eq.${status}`;
    if (pipeline) params['pipeline_id'] = `eq.${pipeline}`;

    const [opportunities, pipelineStages] = await Promise.all([
      supabaseQuery<any>(env, 'opportunities', params),
      supabaseQuery<any>(env, 'pipeline_stages', { select: '*' }),
    ]);

    // Enrich with stage names
    const stageMap = new Map(pipelineStages.map((s: any) => [s.stage_id, s]));
    const enriched = opportunities.map((opp: any) => {
      const stage = stageMap.get(opp.pipeline_stage_id);
      return {
        ...opp,
        pipeline_name: stage?.pipeline_name || 'Unknown',
        stage_name: stage?.stage_name || 'Unknown',
        contact_email: opp.contact?.email,
        contact_phone: opp.contact?.phone,
        contact_tags: opp.contact?.tags,
      };
    });

    return jsonResponse({
      timestamp: new Date().toISOString(),
      count: enriched.length,
      limit,
      offset,
      filters: { status, pipeline },
      data: enriched,
    });
  } catch (error) {
    return errorResponse(`Opportunities error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /analytics/contacts
 * Detailed contact listing with filters
 */
async function getContacts(env: Env, url: URL): Promise<Response> {
  try {
    const tag = url.searchParams.get('tag');
    const source = url.searchParams.get('source');
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 500);
    const offset = parseInt(url.searchParams.get('offset') || '0');

    const params: Record<string, string> = {
      select: 'id,ghl_contact_id,email,phone,first_name,last_name,tags,source,created_at',
      order: 'created_at.desc',
      limit: limit.toString(),
      offset: offset.toString(),
    };

    if (source) params['source'] = `eq.${source}`;

    let contacts = await supabaseQuery<any>(env, 'contacts', params);

    // Filter by tag if specified (PostgREST array contains)
    if (tag) {
      contacts = contacts.filter((c: any) => c.tags?.includes(tag));
    }

    return jsonResponse({
      timestamp: new Date().toISOString(),
      count: contacts.length,
      limit,
      offset,
      filters: { tag, source },
      data: contacts,
    });
  } catch (error) {
    return errorResponse(`Contacts error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * GET /tables
 * List all available tables
 */
async function getTables(env: Env): Promise<Response> {
  try {
    // Fetch the OpenAPI spec which lists all tables
    const response = await fetch(`${env.SUPABASE_URL}/rest/v1/`, {
      headers: {
        apikey: env.SUPABASE_KEY,
        Authorization: `Bearer ${env.SUPABASE_KEY}`,
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch tables: ${response.status}`);
    }

    const spec = await response.json();
    const tables = Object.keys(spec.paths || {})
      .filter((p: string) => p.startsWith('/') && p.length > 1 && !p.includes('/rpc/'))
      .map((p: string) => p.substring(1))
      .sort();

    return jsonResponse({
      timestamp: new Date().toISOString(),
      count: tables.length,
      tables,
    });
  } catch (error) {
    return errorResponse(`Tables error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

/**
 * POST /query
 * Execute a custom query against any table
 */
async function executeQuery(env: Env, request: Request): Promise<Response> {
  try {
    const body = await request.json() as {
      table: string;
      select?: string;
      filters?: Record<string, string>;
      order?: string;
      limit?: number;
      offset?: number;
    };

    if (!body.table) {
      return errorResponse('Missing required field: table', 400);
    }

    const params: Record<string, string> = {};
    if (body.select) params.select = body.select;
    if (body.order) params.order = body.order;
    if (body.limit) params.limit = Math.min(body.limit, 1000).toString();
    if (body.offset) params.offset = body.offset.toString();

    // Add filters
    if (body.filters) {
      Object.entries(body.filters).forEach(([key, value]) => {
        params[key] = value;
      });
    }

    const data = await supabaseQuery<any>(env, body.table, params);

    return jsonResponse({
      timestamp: new Date().toISOString(),
      table: body.table,
      count: data.length,
      data,
    });
  } catch (error) {
    return errorResponse(`Query error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

// ============================================================================
// MAIN ROUTER
// ============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Route requests
      switch (true) {
        case path === '/' || path === '':
          return jsonResponse({
            service: 'PI Analytics Worker',
            version: '1.0.0',
            endpoints: {
              'GET /analytics/summary': 'High-level dashboard metrics',
              'GET /analytics/pipeline': 'Pipeline and stage analysis',
              'GET /analytics/pipeline?pipeline_id=X': 'Specific pipeline analysis',
              'GET /analytics/leads': 'Lead quality and conversion',
              'GET /analytics/attribution': 'Marketing attribution',
              'GET /analytics/data-quality': 'Data quality issues',
              'GET /analytics/migration': 'Salesforce migration status',
              'GET /analytics/opportunities': 'Opportunity listing (supports ?status=X&pipeline=X&limit=N&offset=N)',
              'GET /analytics/contacts': 'Contact listing (supports ?tag=X&source=X&limit=N&offset=N)',
              'GET /tables': 'List all available tables',
              'POST /query': 'Custom query (body: {table, select?, filters?, order?, limit?, offset?})',
            },
          });

        case path === '/analytics/summary':
          return getSummary(env);

        case path === '/analytics/pipeline':
          return getPipelineAnalytics(env, url.searchParams.get('pipeline_id') || undefined);

        case path === '/analytics/leads':
          return getLeadAnalytics(env);

        case path === '/analytics/attribution':
          return getAttributionAnalytics(env);

        case path === '/analytics/data-quality':
          return getDataQualityAnalytics(env);

        case path === '/analytics/migration':
          return getMigrationStatus(env);

        case path === '/analytics/opportunities':
          return getOpportunities(env, url);

        case path === '/analytics/contacts':
          return getContacts(env, url);

        case path === '/tables':
          return getTables(env);

        case path === '/query' && request.method === 'POST':
          return executeQuery(env, request);

        default:
          return errorResponse(`Not found: ${path}`, 404);
      }
    } catch (error) {
      return errorResponse(`Server error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  },
};
