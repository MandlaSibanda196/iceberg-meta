class IcebergMeta < Formula
  include Language::Python::Virtualenv

  desc "CLI and TUI for exploring Apache Iceberg table metadata"
  homepage "https://github.com/MandlaSibanda196/iceberg-meta"
  url "https://github.com/MandlaSibanda196/iceberg-meta/archive/refs/tags/v0.2.5.tar.gz"
  sha256 "66b5922b4f70474f26dd73bc3b87dec46e97b9781f1cc4aa1b762d4961c2852f"
  license "MIT"

  depends_on "python@3.12"

  def install
    virtualenv_install_with_resources
  end

  test do
    system "#{bin}/iceberg-meta", "--help"
  end
end
