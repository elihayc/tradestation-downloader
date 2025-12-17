#!/usr/bin/env python3
"""
TradeStation API Setup Helper
==============================
Helps you get the refresh token needed for automated data downloads.

Usage:
    python setup_auth.py

This script will:
1. Open your browser to authorize the TradeStation app
2. Capture the authorization code
3. Exchange it for a refresh token
4. Save it to config.yaml
"""

import webbrowser
import requests
import yaml
import sys
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# TradeStation OAuth endpoints
AUTHORIZE_URL = "https://signin.tradestation.com/authorize"
TOKEN_URL = "https://signin.tradestation.com/oauth/token"

# Local callback server
CALLBACK_PORT = 3000
REDIRECT_URI = f"http://localhost:{CALLBACK_PORT}"


class CallbackHandler(BaseHTTPRequestHandler):
    """Handle OAuth callback."""
    
    auth_code = None
    
    def do_GET(self):
        """Handle GET request from OAuth callback."""
        parsed = urlparse(self.path)
        
        if parsed.path == "/":
            query_params = parse_qs(parsed.query)
            
            if "code" in query_params:
                CallbackHandler.auth_code = query_params["code"][0]
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"""
                    <html>
                    <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                        <h1>Authorization Successful!</h1>
                        <p>You can close this window and return to the terminal.</p>
                    </body>
                    </html>
                """)
            else:
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                error = query_params.get("error", ["Unknown error"])[0]
                self.wfile.write(f"<html><body>Error: {error}</body></html>".encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        """Suppress logging."""
        pass


def get_authorization_code(client_id: str) -> str:
    """Open browser for user authorization and capture the code."""
    
    # Build authorization URL
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "audience": "https://api.tradestation.com",
        "scope": "openid profile MarketData ReadAccount offline_access"
    }
    auth_url = f"{AUTHORIZE_URL}?{urlencode(params)}"
    
    print("\n" + "="*60)
    print("Step 1: Browser Authorization")
    print("="*60)
    print("\nOpening browser for TradeStation authorization...")
    print("If browser doesn't open, visit this URL manually:")
    print(f"\n{auth_url}\n")
    
    # Start local server to receive callback
    server = HTTPServer(("localhost", CALLBACK_PORT), CallbackHandler)
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()
    
    # Open browser
    webbrowser.open(auth_url)
    
    print("Waiting for authorization...")
    server_thread.join(timeout=300)  # 5 minute timeout
    
    if CallbackHandler.auth_code:
        print("✓ Authorization code received!")
        return CallbackHandler.auth_code
    else:
        raise TimeoutError("Authorization timed out. Please try again.")


def exchange_code_for_tokens(client_id: str, client_secret: str, 
                             auth_code: str) -> dict:
    """Exchange authorization code for access and refresh tokens."""
    
    print("\n" + "="*60)
    print("Step 2: Exchanging Code for Tokens")
    print("="*60)
    
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": auth_code,
        "redirect_uri": REDIRECT_URI
    }
    
    response = requests.post(TOKEN_URL, data=payload)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)
        raise Exception("Failed to exchange code for tokens")
    
    tokens = response.json()
    print("✓ Tokens received!")
    
    return tokens


def save_config(client_id: str, client_secret: str, refresh_token: str):
    """Save configuration to config.yaml."""
    
    config = {
        "tradestation": {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token
        },
        "data_dir": "./data",
        "start_date": "2007-01-01",
        "interval": 1,
        "unit": "Minute",
        "rate_limit_delay": 0.5,
        "max_retries": 3,
        "symbols": [
            "@ES", "@NQ", "@YM", "@RTY",
            "@CL", "@NG",
            "@GC", "@SI",
            "@ZB", "@ZN", "@ZF",
            "@ZC", "@ZS", "@ZW",
            "@6E", "@6J"
        ]
    }
    
    config_path = Path("config.yaml")
    
    if config_path.exists():
        backup_path = Path("config.yaml.backup")
        config_path.rename(backup_path)
        print(f"Existing config backed up to {backup_path}")
    
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print(f"✓ Configuration saved to {config_path}")


def main():
    """Main setup flow."""
    
    print("\n" + "#"*60)
    print("#" + " "*58 + "#")
    print("#    TradeStation API Setup Helper" + " "*23 + "#")
    print("#" + " "*58 + "#")
    print("#"*60)
    
    print("""
This script will help you set up your TradeStation API credentials.

Before running this script, make sure you have:
1. A TradeStation account with API access
2. Created an application at https://developer.tradestation.com/
3. Your Client ID and Client Secret ready

The setup will:
1. Open your browser for TradeStation authorization
2. Capture the tokens automatically
3. Save them to config.yaml
""")
    
    # Get credentials from user
    print("="*60)
    print("Enter Your API Credentials")
    print("="*60)
    
    client_id = input("\nClient ID: ").strip()
    if not client_id:
        print("Error: Client ID is required")
        sys.exit(1)
    
    client_secret = input("Client Secret: ").strip()
    if not client_secret:
        print("Error: Client Secret is required")
        sys.exit(1)
    
    try:
        # Get authorization code
        auth_code = get_authorization_code(client_id)
        
        # Exchange for tokens
        tokens = exchange_code_for_tokens(client_id, client_secret, auth_code)
        
        # Save configuration
        print("\n" + "="*60)
        print("Step 3: Saving Configuration")
        print("="*60)
        
        save_config(client_id, client_secret, tokens["refresh_token"])
        
        print("\n" + "#"*60)
        print("SETUP COMPLETE!")
        print("#"*60)
        print("""
Your TradeStation API is now configured!

Next steps:
1. Review config.yaml and adjust symbols/settings as needed
2. Run the downloader:
   
   python tradestation_downloader.py

3. For incremental updates, just run it again - it will only
   download new data since the last run.

Note: Refresh tokens may expire after extended periods. If you
get authentication errors, run this setup script again.
""")
        
    except KeyboardInterrupt:
        print("\n\nSetup cancelled.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError during setup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
