class IcebergMeta < Formula
  include Language::Python::Virtualenv

  desc "CLI and TUI for exploring Apache Iceberg table metadata"
  homepage "https://github.com/MandlaSibanda196/iceberg-meta"
  url "https://github.com/MandlaSibanda196/iceberg-meta/archive/refs/tags/v0.2.4.tar.gz"
  sha256 "replace_with_actual_sha256"
  license "MIT"

  depends_on "python@3.12"

  def install
    virtualenv_install_with_resources
  end

  test do
    system "#{bin}/iceberg-meta", "--help"
  end
end
