# NM-Bank: Free 24/7 Deploy (Oracle Always Free)

This folder contains ready scripts to deploy NM-Bank to an Oracle Ubuntu VM
with auto-start after reboot (systemd).

## 1) Copy script to your server

From your local machine:

```bash
scp deploy/oracle/install_free_24_7.sh ubuntu@<VM_IP>:/home/ubuntu/
```

Or open SSH and clone the repository directly on the VM.

## 2) Install NM-Bank (first time)

On the VM:

```bash
chmod +x /home/ubuntu/install_free_24_7.sh
sudo bash /home/ubuntu/install_free_24_7.sh https://github.com/demon140582/nm-bank.git
```

After success:

- Health: `http://<VM_IP>:5000/healthz`
- App: `http://<VM_IP>:5000/`

## 3) Update after pushing new commits

```bash
sudo bash /opt/nm-bank/deploy/oracle/update_nm_bank.sh
```

## 4) Useful service commands

```bash
sudo systemctl status nm-bank.service
sudo systemctl restart nm-bank.service
sudo journalctl -u nm-bank.service -f
```

## Notes

- Database is stored in `/var/lib/nm-bank/bank.db` (persistent).
- Service env file is `/etc/nm-bank/nm-bank.env`.
- Gunicorn runs with 1 worker for SQLite safety.
