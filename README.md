# Solana Shred Stream Subscription Test Tool

This tool is used to receive **Shred UDP data streams** from a Solana node, automatically perform packet reconstruction (Reconstruct), missing data recovery (FEC Recovery), deshredding (Deshred), and transaction parsing, and supports subscribing to transaction events for specific accounts.

## Key Features
- **Real-time UDP Reception**: Listens on a specified port to receive shred data from a Solana node.
- **Automatic Reconstruction and Repair**: Uses the Reed-Solomon algorithm to recover lost shreds.
- **Deshredding and Parsing**: Reassembles shreds into complete block entries and parses the transactions within.
- **Account Subscription**: Outputs only the transactions related to the specified account for precise monitoring.

---

## Requirements

- **Rust** 1.80+ (latest stable version recommended)
- **Tokio** asynchronous runtime
- A fully synchronized **Solana node** (or network access to a Solana shred UDP stream)
- System dependencies:

### Ubuntu

```bash
  sudo apt install -y build-essential clang llvm-dev libclang-dev pkg-config libssl-dev
```
### Centos

```bash
  sudo dnf groupinstall -y "Development Tools"
  sudo dnf install -y clang llvm-devel libclang-devel pkgconfig openssl-devel
```

---

## Installation & Compilation

```bash

# Clone the repository
git clone git@github.com:BlockRazorinc/shreds-subscribe.git
cd shreds-subscribe

# Build
# Users can modify the `port` and `subscribe_account` values in `main.rs` according to their development or production environment.
cargo build --release

# Run
./target/release/shreds-subscribe
```