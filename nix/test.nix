{ pkgs, self }:
pkgs.testers.runNixOSTest {
  name = "arctern-service";
  nodes.machine = { ... }: {
    imports = [ self.nixosModules.default ];
    services.arctern.enable = true;
    environment.etc."arctern/arctern.toml".text = "jobs = []\npeers = []\n";
    environment.systemPackages = [ pkgs.curl ];
  };
  testScript = ''
    machine.start()
    machine.wait_for_unit("arctern.service")
    machine.wait_for_open_port(7878)
    machine.succeed("curl -fsS http://127.0.0.1:7878/ | grep -qi '<!doctype html'")
    machine.succeed("test $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:7878/api/v1/jobs) = 401")
    machine.succeed("curl -fsS --unix-socket /run/arctern/arctern.sock http://localhost/api/v1/jobs >/dev/null")
    machine.succeed("test $(stat -c %a /var/lib/arctern) = 700; test $(stat -c %a /var/lib/arctern/admin.token) = 600")
    machine.succeed("systemctl stop arctern")
    machine.fail("test -S /run/arctern/arctern.sock")
  '';
}
