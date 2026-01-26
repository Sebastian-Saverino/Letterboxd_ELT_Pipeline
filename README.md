# Letterboxd_ELT_Pipeline



[Letterboxd_ELT_Data_Pipeline.drawio](https://github.com/user-attachments/files/24850890/Letterboxd_ELT_Data_Pipeline.drawio)
<mxfile host="app.diagrams.net" agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0" version="29.3.4">
  <diagram name="Page-1" id="UVN_MZwssXwiZzHniMjV">
    <mxGraphModel dx="3120" dy="2042" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1100" pageHeight="850" math="0" shadow="0">
      <root>
        <mxCell id="0" />
        <mxCell id="1" parent="0" />
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-22" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-1" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-3" value="Extract">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-1" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Web UI (Upload)" vertex="1">
          <mxGeometry height="60" width="120" x="-570" y="-720" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-23" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-3" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-5" value="Load (Raw Files)">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-24" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-3" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-14" value="Load (Metadata)">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-3" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Fast API&amp;nbsp;&lt;div&gt;(Validate File&lt;/div&gt;&lt;div&gt;Write Metadata)&lt;/div&gt;" vertex="1">
          <mxGeometry height="90" width="120" x="-569" y="-610" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-41" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-5" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=0;entryY=0.5;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-10">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-45" connectable="0" parent="sg9xJ1whNtcHdqzNGkaw-41" style="edgeLabel;html=1;align=center;verticalAlign=middle;resizable=0;points=[];" value="Transform" vertex="1">
          <mxGeometry relative="1" x="-0.0791" y="-2" as="geometry">
            <mxPoint as="offset" />
          </mxGeometry>
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-5" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="MinIO Data Lake (Raw Files)" vertex="1">
          <mxGeometry height="60" width="120" x="-780" y="-455" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-42" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-10" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-16">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-46" connectable="0" parent="sg9xJ1whNtcHdqzNGkaw-42" style="edgeLabel;html=1;align=center;verticalAlign=middle;resizable=0;points=[];" value="Serve" vertex="1">
          <mxGeometry relative="1" x="0.1714" y="-2" as="geometry">
            <mxPoint as="offset" />
          </mxGeometry>
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-10" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Postgres Warehouse (DBT Core)" vertex="1">
          <mxGeometry height="60" width="120" x="-580" y="-320" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-40" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-14" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=1;entryY=0.5;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-10">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-44" connectable="0" parent="sg9xJ1whNtcHdqzNGkaw-40" style="edgeLabel;html=1;align=center;verticalAlign=middle;resizable=0;points=[];" value="Transform" vertex="1">
          <mxGeometry relative="1" x="0.0044" y="1" as="geometry">
            <mxPoint as="offset" />
          </mxGeometry>
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-14" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Postgres(Meta Data)" vertex="1">
          <mxGeometry height="60" width="120" x="-370" y="-455" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-16" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Metabase (Dashboard)" vertex="1">
          <mxGeometry height="60" width="120" x="-580" y="-190" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-21" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-20" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-1" value="Extract">
          <mxGeometry relative="1" as="geometry">
            <Array as="points">
              <mxPoint x="-510" y="-750" />
              <mxPoint x="-510" y="-750" />
            </Array>
          </mxGeometry>
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-20" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Letterboxd Data (From Website)&amp;nbsp;" vertex="1">
          <mxGeometry height="60" width="120" x="-570" y="-820" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-28" parent="1" style="rounded=0;whiteSpace=wrap;html=1;" value="Apache Airflow (Orechestration)" vertex="1">
          <mxGeometry height="60" width="120" x="-1060" y="-430" as="geometry" />
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-36" edge="1" parent="1" source="sg9xJ1whNtcHdqzNGkaw-28" style="endArrow=none;html=1;rounded=0;endFill=0;exitX=1;exitY=0.5;exitDx=0;exitDy=0;" value="">
          <mxGeometry height="50" relative="1" width="50" as="geometry">
            <Array as="points">
              <mxPoint x="-880" y="-400" />
              <mxPoint x="-880" y="-600" />
              <mxPoint x="-880" y="-740" />
            </Array>
            <mxPoint x="-890" y="-390" as="sourcePoint" />
            <mxPoint x="-590" y="-740" as="targetPoint" />
          </mxGeometry>
        </mxCell>
        <mxCell id="sg9xJ1whNtcHdqzNGkaw-38" edge="1" parent="1" style="endArrow=none;html=1;rounded=0;entryX=1;entryY=0.5;entryDx=0;entryDy=0;" target="sg9xJ1whNtcHdqzNGkaw-28" value="">
          <mxGeometry height="50" relative="1" width="50" as="geometry">
            <Array as="points">
              <mxPoint x="-880" y="-80" />
              <mxPoint x="-880" y="-320" />
              <mxPoint x="-880" y="-400" />
            </Array>
            <mxPoint x="-600" y="-80" as="sourcePoint" />
            <mxPoint x="-920" y="-420" as="targetPoint" />
          </mxGeometry>
        </mxCell>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
