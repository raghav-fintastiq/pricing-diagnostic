import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL =
  import.meta.env.VITE_SUPABASE_URL ||
  "https://mjgjpdqnghmitbionhik.supabase.co";

const SUPABASE_ANON_KEY =
  import.meta.env.VITE_SUPABASE_ANON_KEY ||
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1qZ2pwZHFuZ2htaXRiaW9uaGlrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzODM3NTUsImV4cCI6MjA4NTk1OTc1NX0.d0MlYaiokESV7v9a_ECF1SxrepH2_mPRCFw2pWoplJg";

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
