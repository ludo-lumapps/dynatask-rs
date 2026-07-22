echo "alias ll='ls -l'" >> ~/.bashrc
echo "alias ccb='clear && cargo build'" >> ~/.bashrc
echo "alias ccc='clear && cargo clippy'" >> ~/.bashrc
echo "alias ccu='clear && cargo update'" >> ~/.bashrc
echo "alias cct='clear && cargo test'" >> ~/.bashrc

# cargo upgrade
curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
chmod +x /home/vscode/.cargo/bin/cargo-binstall
/home/vscode/.cargo/bin/cargo-binstall cargo-edit -y